// This file is @generated by prost-build.
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct CreateLinkRequest {
    #[prost(string, tag = "1")]
    pub analytics_scope_name: ::prost::alloc::string::String,
    #[prost(string, tag = "2")]
    pub link_name: ::prost::alloc::string::String,
    #[prost(oneof = "create_link_request::Link", tags = "3, 4, 5, 6")]
    pub link: ::core::option::Option<create_link_request::Link>,
}
/// Nested message and enum types in `CreateLinkRequest`.
pub mod create_link_request {
    #[allow(clippy::derive_partial_eq_without_eq)]
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct CouchbaseLink {
        #[prost(string, tag = "1")]
        pub hostname: ::prost::alloc::string::String,
        #[prost(enumeration = "super::CouchbaseEncryptionMode", tag = "2")]
        pub encryption_mode: i32,
        #[prost(string, optional, tag = "3")]
        pub username: ::core::option::Option<::prost::alloc::string::String>,
        #[prost(string, optional, tag = "4")]
        pub password: ::core::option::Option<::prost::alloc::string::String>,
        #[prost(string, optional, tag = "5")]
        pub certificate: ::core::option::Option<::prost::alloc::string::String>,
        #[prost(string, optional, tag = "6")]
        pub client_certificate: ::core::option::Option<::prost::alloc::string::String>,
        #[prost(string, optional, tag = "7")]
        pub client_key: ::core::option::Option<::prost::alloc::string::String>,
    }
    #[allow(clippy::derive_partial_eq_without_eq)]
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct S3Link {
        #[prost(string, tag = "1")]
        pub access_key_id: ::prost::alloc::string::String,
        #[prost(string, tag = "2")]
        pub secret_access_key: ::prost::alloc::string::String,
        #[prost(string, optional, tag = "3")]
        pub session_token: ::core::option::Option<::prost::alloc::string::String>,
        #[prost(string, tag = "4")]
        pub region: ::prost::alloc::string::String,
        #[prost(string, optional, tag = "5")]
        pub service_endpoint: ::core::option::Option<::prost::alloc::string::String>,
    }
    #[allow(clippy::derive_partial_eq_without_eq)]
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct AzureBlobLink {
        #[prost(string, optional, tag = "1")]
        pub account_name: ::core::option::Option<::prost::alloc::string::String>,
        #[prost(string, optional, tag = "2")]
        pub account_key: ::core::option::Option<::prost::alloc::string::String>,
        #[prost(string, optional, tag = "3")]
        pub shared_access_signature: ::core::option::Option<
            ::prost::alloc::string::String,
        >,
        #[prost(string, optional, tag = "4")]
        pub managed_identity_id: ::core::option::Option<::prost::alloc::string::String>,
        #[prost(string, optional, tag = "5")]
        pub client_id: ::core::option::Option<::prost::alloc::string::String>,
        #[prost(string, optional, tag = "6")]
        pub tenant_id: ::core::option::Option<::prost::alloc::string::String>,
        #[prost(string, optional, tag = "7")]
        pub client_secret: ::core::option::Option<::prost::alloc::string::String>,
        #[prost(string, optional, tag = "8")]
        pub client_certificate_password: ::core::option::Option<
            ::prost::alloc::string::String,
        >,
        #[prost(string, optional, tag = "9")]
        pub endpoint: ::core::option::Option<::prost::alloc::string::String>,
    }
    #[allow(clippy::derive_partial_eq_without_eq)]
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct GcsLink {
        #[prost(string, optional, tag = "1")]
        pub json_credentials: ::core::option::Option<::prost::alloc::string::String>,
    }
    #[allow(clippy::derive_partial_eq_without_eq)]
    #[derive(Clone, PartialEq, ::prost::Oneof)]
    pub enum Link {
        #[prost(message, tag = "3")]
        CouchbaseLink(CouchbaseLink),
        #[prost(message, tag = "4")]
        S3Link(S3Link),
        #[prost(message, tag = "5")]
        AzureblobLink(AzureBlobLink),
        #[prost(message, tag = "6")]
        GcsLink(GcsLink),
    }
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, Copy, PartialEq, ::prost::Message)]
pub struct CreateLinkResponse {}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct GetLinkRequest {
    #[prost(string, tag = "1")]
    pub analytics_scope_name: ::prost::alloc::string::String,
    #[prost(string, tag = "2")]
    pub link_name: ::prost::alloc::string::String,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct GetLinkResponse {
    #[prost(oneof = "get_link_response::Link", tags = "1, 2, 3, 4")]
    pub link: ::core::option::Option<get_link_response::Link>,
}
/// Nested message and enum types in `GetLinkResponse`.
pub mod get_link_response {
    #[allow(clippy::derive_partial_eq_without_eq)]
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct CouchbaseLink {
        #[prost(string, tag = "1")]
        pub uuid: ::prost::alloc::string::String,
        #[prost(string, tag = "2")]
        pub active_hostname: ::prost::alloc::string::String,
        #[prost(string, tag = "3")]
        pub bootstrap_hostname: ::prost::alloc::string::String,
        #[prost(bool, tag = "4")]
        pub is_bootstrap_alternate_address: bool,
        #[prost(string, optional, tag = "5")]
        pub certificate: ::core::option::Option<::prost::alloc::string::String>,
        /// client_certificate
        /// client_key
        /// cluster_compatibility
        #[prost(enumeration = "super::CouchbaseEncryptionMode", tag = "6")]
        pub encryption_mode: i32,
        /// nodes
        ///
        /// password
        #[prost(string, tag = "7")]
        pub username: ::prost::alloc::string::String,
    }
    #[allow(clippy::derive_partial_eq_without_eq)]
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct S3Link {
        #[prost(string, tag = "1")]
        pub access_key_id: ::prost::alloc::string::String,
        #[prost(string, tag = "2")]
        pub region: ::prost::alloc::string::String,
        /// secret_access_key
        /// session_token
        #[prost(string, optional, tag = "3")]
        pub service_endpoint: ::core::option::Option<::prost::alloc::string::String>,
    }
    #[allow(clippy::derive_partial_eq_without_eq)]
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct AzureBlobLink {
        /// account_key
        #[prost(string, optional, tag = "1")]
        pub account_name: ::core::option::Option<::prost::alloc::string::String>,
        /// client_certificate
        /// client_certificate_password
        #[prost(string, optional, tag = "2")]
        pub client_id: ::core::option::Option<::prost::alloc::string::String>,
        /// client_secret
        #[prost(string, tag = "3")]
        pub endpoint: ::prost::alloc::string::String,
        #[prost(string, optional, tag = "4")]
        pub managed_identity_id: ::core::option::Option<::prost::alloc::string::String>,
        /// shared_access_signature
        #[prost(string, optional, tag = "5")]
        pub tenant_id: ::core::option::Option<::prost::alloc::string::String>,
    }
    #[allow(clippy::derive_partial_eq_without_eq)]
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct GcsLink {
        #[prost(bool, tag = "1")]
        pub use_application_default_credentials: bool,
        /// json_credentials
        #[prost(string, tag = "2")]
        pub endpoint: ::prost::alloc::string::String,
    }
    #[allow(clippy::derive_partial_eq_without_eq)]
    #[derive(Clone, PartialEq, ::prost::Oneof)]
    pub enum Link {
        #[prost(message, tag = "1")]
        CouchbaseLink(CouchbaseLink),
        #[prost(message, tag = "2")]
        S3Link(S3Link),
        #[prost(message, tag = "3")]
        AzureblobLink(AzureBlobLink),
        #[prost(message, tag = "4")]
        GcsLink(GcsLink),
    }
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct UpdateLinkRequest {
    #[prost(string, tag = "1")]
    pub analytics_scope_name: ::prost::alloc::string::String,
    #[prost(string, tag = "2")]
    pub link_name: ::prost::alloc::string::String,
    #[prost(oneof = "update_link_request::Link", tags = "3, 4, 5, 6")]
    pub link: ::core::option::Option<update_link_request::Link>,
}
/// Nested message and enum types in `UpdateLinkRequest`.
pub mod update_link_request {
    #[allow(clippy::derive_partial_eq_without_eq)]
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct CouchbaseLink {
        #[prost(string, tag = "1")]
        pub hostname: ::prost::alloc::string::String,
        #[prost(enumeration = "super::CouchbaseEncryptionMode", tag = "2")]
        pub encryption_mode: i32,
        #[prost(string, optional, tag = "3")]
        pub username: ::core::option::Option<::prost::alloc::string::String>,
        #[prost(string, optional, tag = "4")]
        pub password: ::core::option::Option<::prost::alloc::string::String>,
        #[prost(string, optional, tag = "5")]
        pub certificate: ::core::option::Option<::prost::alloc::string::String>,
        #[prost(string, optional, tag = "6")]
        pub client_certificate: ::core::option::Option<::prost::alloc::string::String>,
        #[prost(string, optional, tag = "7")]
        pub client_key: ::core::option::Option<::prost::alloc::string::String>,
    }
    #[allow(clippy::derive_partial_eq_without_eq)]
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct S3Link {
        #[prost(string, tag = "1")]
        pub access_key_id: ::prost::alloc::string::String,
        #[prost(string, tag = "2")]
        pub secret_access_key: ::prost::alloc::string::String,
        #[prost(string, optional, tag = "3")]
        pub session_token: ::core::option::Option<::prost::alloc::string::String>,
        #[prost(string, tag = "4")]
        pub region: ::prost::alloc::string::String,
        #[prost(string, optional, tag = "5")]
        pub service_endpoint: ::core::option::Option<::prost::alloc::string::String>,
    }
    #[allow(clippy::derive_partial_eq_without_eq)]
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct AzureBlobLink {
        #[prost(string, optional, tag = "1")]
        pub account_name: ::core::option::Option<::prost::alloc::string::String>,
        #[prost(string, optional, tag = "2")]
        pub account_key: ::core::option::Option<::prost::alloc::string::String>,
        #[prost(string, optional, tag = "3")]
        pub shared_access_signature: ::core::option::Option<
            ::prost::alloc::string::String,
        >,
        #[prost(string, optional, tag = "4")]
        pub managed_identity_id: ::core::option::Option<::prost::alloc::string::String>,
        #[prost(string, optional, tag = "5")]
        pub client_id: ::core::option::Option<::prost::alloc::string::String>,
        #[prost(string, optional, tag = "6")]
        pub tenant_id: ::core::option::Option<::prost::alloc::string::String>,
        #[prost(string, optional, tag = "7")]
        pub client_secret: ::core::option::Option<::prost::alloc::string::String>,
        #[prost(string, optional, tag = "8")]
        pub client_certificate_password: ::core::option::Option<
            ::prost::alloc::string::String,
        >,
        #[prost(string, optional, tag = "9")]
        pub endpoint: ::core::option::Option<::prost::alloc::string::String>,
    }
    #[allow(clippy::derive_partial_eq_without_eq)]
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct GcsLink {
        #[prost(string, optional, tag = "1")]
        pub json_credentials: ::core::option::Option<::prost::alloc::string::String>,
    }
    #[allow(clippy::derive_partial_eq_without_eq)]
    #[derive(Clone, PartialEq, ::prost::Oneof)]
    pub enum Link {
        #[prost(message, tag = "3")]
        CouchbaseLink(CouchbaseLink),
        #[prost(message, tag = "4")]
        S3Link(S3Link),
        #[prost(message, tag = "5")]
        AzureblobLink(AzureBlobLink),
        #[prost(message, tag = "6")]
        GcsLink(GcsLink),
    }
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, Copy, PartialEq, ::prost::Message)]
pub struct UpdateLinkResponse {}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct DeleteLinkRequest {
    #[prost(string, tag = "1")]
    pub analytics_scope_name: ::prost::alloc::string::String,
    #[prost(string, tag = "2")]
    pub link_name: ::prost::alloc::string::String,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, Copy, PartialEq, ::prost::Message)]
pub struct DeleteLinkResponse {}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, Copy, PartialEq, ::prost::Message)]
pub struct GetIngestionStatusRequest {}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct GetIngestionStatusResponse {
    #[prost(message, repeated, tag = "1")]
    pub links: ::prost::alloc::vec::Vec<get_ingestion_status_response::Link>,
}
/// Nested message and enum types in `GetIngestionStatusResponse`.
pub mod get_ingestion_status_response {
    #[allow(clippy::derive_partial_eq_without_eq)]
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct Link {
        #[prost(string, tag = "1")]
        pub link_name: ::prost::alloc::string::String,
        #[prost(string, tag = "2")]
        pub analytics_scope_name: ::prost::alloc::string::String,
        #[prost(enumeration = "super::LinkStatus", tag = "3")]
        pub status: i32,
        #[prost(message, repeated, tag = "4")]
        pub states: ::prost::alloc::vec::Vec<link::State>,
    }
    /// Nested message and enum types in `Link`.
    pub mod link {
        #[allow(clippy::derive_partial_eq_without_eq)]
        #[derive(Clone, PartialEq, ::prost::Message)]
        pub struct State {
            #[prost(message, optional, tag = "1")]
            pub timestamp: ::core::option::Option<::prost_types::Timestamp>,
            #[prost(double, tag = "2")]
            pub progress: f64,
            #[prost(int64, optional, tag = "3")]
            pub time_lag_ms: ::core::option::Option<i64>,
            #[prost(int64, optional, tag = "4")]
            pub items_processed: ::core::option::Option<i64>,
            #[prost(int64, optional, tag = "5")]
            pub seqno_advances: ::core::option::Option<i64>,
            #[prost(message, repeated, tag = "6")]
            pub scopes: ::prost::alloc::vec::Vec<state::Scope>,
        }
        /// Nested message and enum types in `State`.
        pub mod state {
            #[allow(clippy::derive_partial_eq_without_eq)]
            #[derive(Clone, PartialEq, ::prost::Message)]
            pub struct Scope {
                #[prost(string, tag = "1")]
                pub scope_name: ::prost::alloc::string::String,
                #[prost(message, repeated, tag = "2")]
                pub collections: ::prost::alloc::vec::Vec<scope::Collection>,
            }
            /// Nested message and enum types in `Scope`.
            pub mod scope {
                #[allow(clippy::derive_partial_eq_without_eq)]
                #[derive(Clone, PartialEq, ::prost::Message)]
                pub struct Collection {
                    #[prost(string, tag = "1")]
                    pub collection_name: ::prost::alloc::string::String,
                }
            }
        }
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
#[repr(i32)]
pub enum LinkStatus {
    Unknown = 0,
    Healthy = 1,
    Stopped = 2,
    Unhealthy = 3,
    Suspended = 4,
}
impl LinkStatus {
    /// String value of the enum field names used in the ProtoBuf definition.
    ///
    /// The values are not transformed in any way and thus are considered stable
    /// (if the ProtoBuf definition does not change) and safe for programmatic use.
    pub fn as_str_name(&self) -> &'static str {
        match self {
            LinkStatus::Unknown => "LINK_STATUS_UNKNOWN",
            LinkStatus::Healthy => "LINK_STATUS_HEALTHY",
            LinkStatus::Stopped => "LINK_STATUS_STOPPED",
            LinkStatus::Unhealthy => "LINK_STATUS_UNHEALTHY",
            LinkStatus::Suspended => "LINK_STATUS_SUSPENDED",
        }
    }
    /// Creates an enum from field names used in the ProtoBuf definition.
    pub fn from_str_name(value: &str) -> ::core::option::Option<Self> {
        match value {
            "LINK_STATUS_UNKNOWN" => Some(Self::Unknown),
            "LINK_STATUS_HEALTHY" => Some(Self::Healthy),
            "LINK_STATUS_STOPPED" => Some(Self::Stopped),
            "LINK_STATUS_UNHEALTHY" => Some(Self::Unhealthy),
            "LINK_STATUS_SUSPENDED" => Some(Self::Suspended),
            _ => None,
        }
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
#[repr(i32)]
pub enum CouchbaseEncryptionMode {
    None = 0,
    Half = 1,
    Full = 2,
}
impl CouchbaseEncryptionMode {
    /// String value of the enum field names used in the ProtoBuf definition.
    ///
    /// The values are not transformed in any way and thus are considered stable
    /// (if the ProtoBuf definition does not change) and safe for programmatic use.
    pub fn as_str_name(&self) -> &'static str {
        match self {
            CouchbaseEncryptionMode::None => "COUCHBASE_ENCRYPTION_MODE_NONE",
            CouchbaseEncryptionMode::Half => "COUCHBASE_ENCRYPTION_MODE_HALF",
            CouchbaseEncryptionMode::Full => "COUCHBASE_ENCRYPTION_MODE_FULL",
        }
    }
    /// Creates an enum from field names used in the ProtoBuf definition.
    pub fn from_str_name(value: &str) -> ::core::option::Option<Self> {
        match value {
            "COUCHBASE_ENCRYPTION_MODE_NONE" => Some(Self::None),
            "COUCHBASE_ENCRYPTION_MODE_HALF" => Some(Self::Half),
            "COUCHBASE_ENCRYPTION_MODE_FULL" => Some(Self::Full),
            _ => None,
        }
    }
}
/// Generated client implementations.
pub mod analytics_admin_service_client {
    #![allow(unused_variables, dead_code, missing_docs, clippy::let_unit_value)]
    use tonic::codegen::*;
    use tonic::codegen::http::Uri;
    #[derive(Debug, Clone)]
    pub struct AnalyticsAdminServiceClient<T> {
        inner: tonic::client::Grpc<T>,
    }
    impl AnalyticsAdminServiceClient<tonic::transport::Channel> {
        /// Attempt to create a new client by connecting to a given endpoint.
        pub async fn connect<D>(dst: D) -> Result<Self, tonic::transport::Error>
        where
            D: TryInto<tonic::transport::Endpoint>,
            D::Error: Into<StdError>,
        {
            let conn = tonic::transport::Endpoint::new(dst)?.connect().await?;
            Ok(Self::new(conn))
        }
    }
    impl<T> AnalyticsAdminServiceClient<T>
    where
        T: tonic::client::GrpcService<tonic::body::BoxBody>,
        T::Error: Into<StdError>,
        T::ResponseBody: Body<Data = Bytes> + Send + 'static,
        <T::ResponseBody as Body>::Error: Into<StdError> + Send,
    {
        pub fn new(inner: T) -> Self {
            let inner = tonic::client::Grpc::new(inner);
            Self { inner }
        }
        pub fn with_origin(inner: T, origin: Uri) -> Self {
            let inner = tonic::client::Grpc::with_origin(inner, origin);
            Self { inner }
        }
        pub fn with_interceptor<F>(
            inner: T,
            interceptor: F,
        ) -> AnalyticsAdminServiceClient<InterceptedService<T, F>>
        where
            F: tonic::service::Interceptor,
            T::ResponseBody: Default,
            T: tonic::codegen::Service<
                http::Request<tonic::body::BoxBody>,
                Response = http::Response<
                    <T as tonic::client::GrpcService<tonic::body::BoxBody>>::ResponseBody,
                >,
            >,
            <T as tonic::codegen::Service<
                http::Request<tonic::body::BoxBody>,
            >>::Error: Into<StdError> + Send + Sync,
        {
            AnalyticsAdminServiceClient::new(InterceptedService::new(inner, interceptor))
        }
        /// Compress requests with the given encoding.
        ///
        /// This requires the server to support it otherwise it might respond with an
        /// error.
        #[must_use]
        pub fn send_compressed(mut self, encoding: CompressionEncoding) -> Self {
            self.inner = self.inner.send_compressed(encoding);
            self
        }
        /// Enable decompressing responses.
        #[must_use]
        pub fn accept_compressed(mut self, encoding: CompressionEncoding) -> Self {
            self.inner = self.inner.accept_compressed(encoding);
            self
        }
        /// Limits the maximum size of a decoded message.
        ///
        /// Default: `4MB`
        #[must_use]
        pub fn max_decoding_message_size(mut self, limit: usize) -> Self {
            self.inner = self.inner.max_decoding_message_size(limit);
            self
        }
        /// Limits the maximum size of an encoded message.
        ///
        /// Default: `usize::MAX`
        #[must_use]
        pub fn max_encoding_message_size(mut self, limit: usize) -> Self {
            self.inner = self.inner.max_encoding_message_size(limit);
            self
        }
        pub async fn create_link(
            &mut self,
            request: impl tonic::IntoRequest<super::CreateLinkRequest>,
        ) -> std::result::Result<
            tonic::Response<super::CreateLinkResponse>,
            tonic::Status,
        > {
            self.inner
                .ready()
                .await
                .map_err(|e| {
                    tonic::Status::new(
                        tonic::Code::Unknown,
                        format!("Service was not ready: {}", e.into()),
                    )
                })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static(
                "/couchbase.admin.analytics.v1.AnalyticsAdminService/CreateLink",
            );
            let mut req = request.into_request();
            req.extensions_mut()
                .insert(
                    GrpcMethod::new(
                        "couchbase.admin.analytics.v1.AnalyticsAdminService",
                        "CreateLink",
                    ),
                );
            self.inner.unary(req, path, codec).await
        }
        pub async fn get_link(
            &mut self,
            request: impl tonic::IntoRequest<super::GetLinkRequest>,
        ) -> std::result::Result<
            tonic::Response<super::GetLinkResponse>,
            tonic::Status,
        > {
            self.inner
                .ready()
                .await
                .map_err(|e| {
                    tonic::Status::new(
                        tonic::Code::Unknown,
                        format!("Service was not ready: {}", e.into()),
                    )
                })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static(
                "/couchbase.admin.analytics.v1.AnalyticsAdminService/GetLink",
            );
            let mut req = request.into_request();
            req.extensions_mut()
                .insert(
                    GrpcMethod::new(
                        "couchbase.admin.analytics.v1.AnalyticsAdminService",
                        "GetLink",
                    ),
                );
            self.inner.unary(req, path, codec).await
        }
        pub async fn update_link(
            &mut self,
            request: impl tonic::IntoRequest<super::UpdateLinkRequest>,
        ) -> std::result::Result<
            tonic::Response<super::UpdateLinkResponse>,
            tonic::Status,
        > {
            self.inner
                .ready()
                .await
                .map_err(|e| {
                    tonic::Status::new(
                        tonic::Code::Unknown,
                        format!("Service was not ready: {}", e.into()),
                    )
                })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static(
                "/couchbase.admin.analytics.v1.AnalyticsAdminService/UpdateLink",
            );
            let mut req = request.into_request();
            req.extensions_mut()
                .insert(
                    GrpcMethod::new(
                        "couchbase.admin.analytics.v1.AnalyticsAdminService",
                        "UpdateLink",
                    ),
                );
            self.inner.unary(req, path, codec).await
        }
        pub async fn delete_link(
            &mut self,
            request: impl tonic::IntoRequest<super::DeleteLinkRequest>,
        ) -> std::result::Result<
            tonic::Response<super::DeleteLinkResponse>,
            tonic::Status,
        > {
            self.inner
                .ready()
                .await
                .map_err(|e| {
                    tonic::Status::new(
                        tonic::Code::Unknown,
                        format!("Service was not ready: {}", e.into()),
                    )
                })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static(
                "/couchbase.admin.analytics.v1.AnalyticsAdminService/DeleteLink",
            );
            let mut req = request.into_request();
            req.extensions_mut()
                .insert(
                    GrpcMethod::new(
                        "couchbase.admin.analytics.v1.AnalyticsAdminService",
                        "DeleteLink",
                    ),
                );
            self.inner.unary(req, path, codec).await
        }
        pub async fn get_ingestion_status(
            &mut self,
            request: impl tonic::IntoRequest<super::GetIngestionStatusRequest>,
        ) -> std::result::Result<
            tonic::Response<super::GetIngestionStatusResponse>,
            tonic::Status,
        > {
            self.inner
                .ready()
                .await
                .map_err(|e| {
                    tonic::Status::new(
                        tonic::Code::Unknown,
                        format!("Service was not ready: {}", e.into()),
                    )
                })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static(
                "/couchbase.admin.analytics.v1.AnalyticsAdminService/GetIngestionStatus",
            );
            let mut req = request.into_request();
            req.extensions_mut()
                .insert(
                    GrpcMethod::new(
                        "couchbase.admin.analytics.v1.AnalyticsAdminService",
                        "GetIngestionStatus",
                    ),
                );
            self.inner.unary(req, path, codec).await
        }
    }
}