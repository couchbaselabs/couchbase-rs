/*
 *
 *  * Copyright (c) 2025 Couchbase, Inc.
 *  *
 *  * Licensed under the Apache License, Version 2.0 (the "License");
 *  * you may not use this file except in compliance with the License.
 *  * You may obtain a copy of the License at
 *  *
 *  *    http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use arc_swap::ArcSwap;
use tokio::sync::Notify;

use crate::collectionresolver::CollectionResolver;
use crate::error::Error;
use crate::error::Result;

struct CollectionsFastCacheEntry {
    pub collection_id: u32,
    pub manifest_rev: u64,
}

#[derive(Default)]
struct CollectionsFastManifest {
    pub collections: HashMap<String, CollectionsFastCacheEntry>,
}

#[derive(Clone)]
struct CollectionCacheEntry {
    // TODO: Strongly suspect these should be Option.
    collection_id: u32,
    manifest_rev: u64,

    pending: Option<Arc<Notify>>,
}

type CollectionResolverSlowMap = Arc<Mutex<HashMap<String, Arc<Mutex<CollectionCacheEntry>>>>>;

pub(crate) struct CollectionResolverCached<Resolver: CollectionResolver> {
    resolver: Arc<Resolver>,

    fast_cache: Arc<ArcSwap<CollectionsFastManifest>>,

    slow_map: CollectionResolverSlowMap,
}

#[derive(Clone)]
pub(crate) struct CollectionResolverCachedOptions<Resolver: CollectionResolver> {
    pub resolver: Resolver,
}

impl<Resolver> CollectionResolverCached<Resolver>
where
    Resolver: CollectionResolver + 'static,
{
    pub fn new(opts: CollectionResolverCachedOptions<Resolver>) -> Self {
        Self {
            resolver: Arc::new(opts.resolver),
            fast_cache: Arc::new(ArcSwap::from_pointee(Default::default())),
            slow_map: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    async fn resolve_collection_id_slow(
        &self,
        scope_name: &str,
        collection_name: &str,
        full_key_path: &str,
    ) -> Result<(u32, u64)> {
        loop {
            // This pending logic is a little convoluted but without it the compiler
            // thinks that we are holding the slow_map lock across an await, even
            // if we drop it manually.
            let pending = {
                let mut slow_map = self.slow_map.lock().unwrap();

                if let Some(entry) = slow_map.get(full_key_path) {
                    let entry_guard = entry.lock().unwrap();
                    if let Some(pending) = &entry_guard.pending {
                        Some(pending.clone())
                    } else {
                        return Ok((entry_guard.collection_id, entry_guard.manifest_rev));
                    }
                } else {
                    let entry = Arc::new(Mutex::new(CollectionCacheEntry {
                        collection_id: 0,
                        manifest_rev: 0,
                        pending: Some(Arc::new(Notify::new())),
                    }));

                    slow_map.insert(full_key_path.to_string(), entry);

                    None
                }
            };

            if let Some(p) = pending {
                p.notified().await;

                continue;
            }

            let resolve_resp = {
                let scope_name = scope_name.to_string();
                let collection_name = collection_name.to_string();
                let resolver = self.resolver.clone();
                let handle = tokio::spawn(async move {
                    resolver
                        .resolve_collection_id(&scope_name, &collection_name)
                        .await
                });

                handle.await
            }
            .map_err(|e| {
                Error::new_message_error(format!("failed to join resolve collection id task: {e}"))
            })?;

            return match resolve_resp {
                Ok((collection_id, manifest_rev)) => {
                    let slow_map = self.slow_map.lock().unwrap();
                    let entry = slow_map
                        .get(full_key_path)
                        .expect("slow map was missing collection id entry");

                    let pending = {
                        let mut guard = entry.lock().unwrap();
                        guard.collection_id = collection_id;
                        guard.manifest_rev = manifest_rev;

                        guard.pending.take()
                    };

                    Self::rebuild_fast_cache_locked(&slow_map, self.fast_cache.clone());

                    if let Some(p) = pending {
                        p.notify_waiters();
                    }

                    Ok((collection_id, manifest_rev))
                }
                Err(e) => {
                    let mut slow_map = self.slow_map.lock().unwrap();
                    let entry = slow_map
                        .remove(full_key_path)
                        .expect("slow map was missing collection id entry");

                    let mut guard = entry.lock().unwrap();
                    let pending = guard.pending.take();

                    // No need to rebuild the fast cache as we haven't added this entry to it.

                    if let Some(p) = pending {
                        p.notify_waiters();
                    }

                    Err(e)
                }
            };
        }
    }

    fn rebuild_fast_cache_locked(
        guard: &HashMap<String, Arc<Mutex<CollectionCacheEntry>>>,
        fast_cache: Arc<ArcSwap<CollectionsFastManifest>>,
    ) {
        let mut collections = HashMap::new();
        for (full_key_path, entry) in guard.iter() {
            let (collection_id, manifest_rev) = {
                let guard = entry.lock().unwrap();
                (guard.collection_id, guard.manifest_rev)
            };

            if collection_id > 0 {
                collections.insert(
                    full_key_path.clone(),
                    CollectionsFastCacheEntry {
                        collection_id,
                        manifest_rev,
                    },
                );
            }
        }

        fast_cache.store(Arc::new(CollectionsFastManifest { collections }));
    }
}

impl<Resolver> CollectionResolver for CollectionResolverCached<Resolver>
where
    Resolver: CollectionResolver + 'static,
{
    async fn resolve_collection_id(
        &self,
        scope_name: &str,
        collection_name: &str,
    ) -> Result<(u32, u64)> {
        let full_key_path = scope_name.to_owned() + "." + collection_name;

        {
            let fast_cache = self.fast_cache.load();
            if let Some(entry) = fast_cache.collections.get(&full_key_path) {
                return Ok((entry.collection_id, entry.manifest_rev));
            }
        }

        self.resolve_collection_id_slow(scope_name, collection_name, &full_key_path)
            .await
    }

    async fn invalidate_collection_id(&self, scope_name: &str, collection_name: &str) {
        self.resolver
            .invalidate_collection_id(scope_name, collection_name)
            .await;

        let full_key_path = scope_name.to_owned() + "." + collection_name;

        let mut slow_map = self.slow_map.lock().unwrap();
        slow_map.remove(&full_key_path);

        Self::rebuild_fast_cache_locked(&slow_map, self.fast_cache.clone());
    }
}
