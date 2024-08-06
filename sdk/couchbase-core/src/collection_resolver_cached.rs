use std::collections::HashMap;
use std::sync::Arc;

use arc_swap::ArcSwap;
use tokio::select;
use tokio::sync::{Mutex, Notify};

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
    resolve_err: Option<Error>,

    // TODO: Strongly suspect these should be Option.
    collection_id: u32,
    manifest_rev: u64,

    pending: Option<Arc<Notify>>,
    done: Option<Arc<Notify>>,
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
        let mut slow_map = self.slow_map.lock().await;

        let slow_entry = if let Some(entry) = slow_map.get(full_key_path) {
            entry
        } else {
            let entry = CollectionCacheEntry {
                resolve_err: None,
                collection_id: 0,
                manifest_rev: 0,
                pending: Some(Arc::new(Notify::new())),
                done: Some(Arc::new(Notify::new())),
            };

            slow_map.insert(full_key_path.to_string(), Arc::new(Mutex::new(entry)));

            // TODO: Probably a better way to do this.
            let entry = slow_map.get(full_key_path).unwrap();

            {
                let s_map = self.slow_map.clone();
                let scope_name = scope_name.to_string();
                let collection_name = collection_name.to_string();
                let fast_cache = self.fast_cache.clone();
                let resolver = self.resolver.clone();
                let entry = entry.clone();

                tokio::spawn(Self::resolve_collection(
                    entry,
                    s_map,
                    fast_cache,
                    resolver,
                    scope_name,
                    collection_name,
                ));
            }

            entry
        };

        let entry_guard = slow_entry.lock().await;
        if let Some(pending) = &entry_guard.pending {
            let pending = pending.clone();
            drop(entry_guard);
            drop(slow_map);

            select! {
                () = pending.notified() => {}
            }

            return Box::pin(self.resolve_collection_id_slow(
                scope_name,
                collection_name,
                full_key_path,
            ))
            .await;
        }

        if let Some(e) = &entry_guard.resolve_err {
            return Err(e.clone());
        }

        Ok((entry_guard.collection_id, entry_guard.manifest_rev))
    }

    async fn resolve_collection(
        entry: Arc<Mutex<CollectionCacheEntry>>,
        slow_map: CollectionResolverSlowMap,
        fast_cache: Arc<ArcSwap<CollectionsFastManifest>>,
        resolver: Arc<Resolver>,
        scope_name: String,
        collection_name: String,
    ) {
        loop {
            let (collection_id, manifest_rev) = match resolver
                .resolve_collection_id(&scope_name, &collection_name)
                .await
            {
                Ok((id, rev)) => (id, rev),
                Err(e) => {
                    {
                        let mut guard = entry.lock().await;
                        guard.resolve_err = Some(e);
                        guard.collection_id = 0;

                        let pending = guard.pending.clone();
                        guard.pending = None;

                        if let Some(p) = pending {
                            p.notify_waiters();
                        }
                    }

                    Self::rebuild_fast_cache(slow_map.clone(), fast_cache.clone()).await;

                    continue;
                }
            };

            let (pending, done) = {
                let mut guard = entry.lock().await;
                guard.resolve_err = None;
                guard.collection_id = collection_id;
                guard.manifest_rev = manifest_rev;

                let done = guard.done.clone();
                guard.done = None;

                let pending = guard.pending.clone();
                guard.pending = None;

                (pending, done)
            };

            Self::rebuild_fast_cache(slow_map.clone(), fast_cache.clone()).await;

            if let Some(p) = pending {
                p.notify_waiters();
            }

            if let Some(d) = done {
                d.notify_waiters();
            }

            return;
        }
    }

    async fn rebuild_fast_cache(
        slow_map: CollectionResolverSlowMap,
        fast_cache: Arc<ArcSwap<CollectionsFastManifest>>,
    ) {
        let guard = slow_map.lock().await;

        let mut collections = HashMap::new();
        for (full_key_path, entry) in guard.iter() {
            let (collection_id, manifest_rev) = {
                let guard = entry.lock().await;
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

        let fast_cache = self.fast_cache.load();
        if let Some(entry) = fast_cache.collections.get(&full_key_path) {
            return Ok((entry.collection_id, entry.manifest_rev));
        }

        self.resolve_collection_id_slow(scope_name, collection_name, &full_key_path)
            .await
    }

    async fn invalidate_collection_id(
        &self,
        scope_name: &str,
        collection_name: &str,
        endpoint: &str,
        invalidating_manifest_rev: u64,
    ) {
        self.resolver
            .invalidate_collection_id(
                scope_name,
                collection_name,
                endpoint,
                invalidating_manifest_rev,
            )
            .await;

        let full_key_path = scope_name.to_owned() + "." + collection_name;

        let slow_map = self.slow_map.lock().await;
        let entry = if let Some(entry) = slow_map.get(&full_key_path) {
            entry
        } else {
            return;
        };

        {
            let mut entry_guard = entry.lock().await;

            if entry_guard.done.is_some() {
                // our entry is already being refetched, no need to do anything
                return;
            }

            if entry_guard.manifest_rev > invalidating_manifest_rev {
                // our entry is newer than is being invalidated, so leave it...
                return;
            }

            // Reset the entry
            entry_guard.resolve_err = None;
            entry_guard.manifest_rev = 0;
            entry_guard.collection_id = 0;

            if entry_guard.pending.is_none() {
                entry_guard.pending = Some(Arc::new(Notify::new()));
            }

            if entry_guard.done.is_none() {
                entry_guard.done = Some(Arc::new(Notify::new()));

                {
                    let s_map = self.slow_map.clone();
                    let scope_name = scope_name.to_string();
                    let collection_name = collection_name.to_string();
                    let fast_cache = self.fast_cache.clone();
                    let resolver = self.resolver.clone();
                    let entry = entry.clone();

                    tokio::spawn(Self::resolve_collection(
                        entry,
                        s_map,
                        fast_cache,
                        resolver,
                        scope_name,
                        collection_name,
                    ));
                }
            }
        }

        Self::rebuild_fast_cache(self.slow_map.clone(), self.fast_cache.clone()).await;
    }
}
