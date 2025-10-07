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

#![allow(clippy::all)]

pub mod couchbase {
    pub mod admin {
        pub mod bucket {
            pub mod v1 {
                include!("../genproto/couchbase.admin.bucket.v1.rs");
            }
        }
        pub mod collection {
            pub mod v1 {
                include!("../genproto/couchbase.admin.collection.v1.rs");
            }
        }
        pub mod query {
            pub mod v1 {
                include!("../genproto/couchbase.admin.query.v1.rs");
            }
        }
        pub mod search {
            pub mod v1 {
                include!("../genproto/couchbase.admin.search.v1.rs");
            }
        }
    }
    pub mod internal {
        pub mod hooks {
            pub mod v1 {
                include!("../genproto/couchbase.internal.hooks.v1.rs");
            }
        }
    }
    pub mod kv {
        pub mod v1 {
            include!("../genproto/couchbase.kv.v1.rs");
        }
    }
    pub mod query {
        pub mod v1 {
            include!("../genproto/couchbase.query.v1.rs");
        }
    }
    pub mod routing {
        pub mod v1 {
            include!("../genproto/couchbase.routing.v1.rs");
        }
    }
    pub mod search {
        pub mod v1 {
            include!("../genproto/couchbase.search.v1.rs");
        }
    }
    pub mod transactions {
        pub mod v1 {
            include!("../genproto/couchbase.transactions.v1.rs");
        }
    }
}

pub mod google {
    pub mod rpc {
        include!("../genproto/google.rpc.rs");
    }
}
