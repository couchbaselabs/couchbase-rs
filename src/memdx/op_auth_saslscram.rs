use scram_rs::{
    ScramAuthClient, ScramCbHelper, ScramKey, ScramNonce, ScramSha256RustNative,
};
use scram_rs::scram_sync::SyncScramClient;

use crate::memdx::auth_mechanism::AuthMechanism;
use crate::memdx::client::Result;
use crate::memdx::dispatcher::Dispatcher;
use crate::memdx::op_bootstrap::OpAuthEncoder;
use crate::memdx::ops_core::OpsCore;
use crate::memdx::pendingop::SASLAuthScramPendingOp;
use crate::memdx::request::SASLAuthRequest;

#[derive(Copy, Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub enum SASLAuthScramHash {
    ScramSha1,
    ScramSha256,
    ScramSha512,
}

impl Into<AuthMechanism> for SASLAuthScramHash {
    fn into(self) -> AuthMechanism {
        match self {
            SASLAuthScramHash::ScramSha1 => AuthMechanism::ScramSha1,
            SASLAuthScramHash::ScramSha256 => AuthMechanism::ScramSha256,
            SASLAuthScramHash::ScramSha512 => AuthMechanism::ScramSha512,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct SASLAuthScramOptions {
    username: String,
    password: String,
    hash: SASLAuthScramHash,
}

impl SASLAuthScramOptions {
    pub fn new(username: String, password: String, hash: SASLAuthScramHash) -> Self {
        Self {
            username,
            password,
            hash,
        }
    }
    pub fn username(&self) -> String {
        self.username.clone()
    }
    pub fn password(&self) -> String {
        self.password.clone()
    }
    pub fn hash(&self) -> SASLAuthScramHash {
        self.hash
    }
}

struct AuthClient {
    username: String,
    password: String,
    key: ScramKey,
}

impl AuthClient {
    pub fn new(username: String, password: String) -> Self {
        Self {
            username,
            password,
            key: ScramKey::new(),
        }
    }
}

impl ScramAuthClient for AuthClient {
    fn get_username(&self) -> &str {
        return &self.username;
    }

    fn get_password(&self) -> &str {
        return &self.password;
    }

    fn get_scram_keys(&self) -> &ScramKey {
        return &self.key;
    }
}

impl ScramCbHelper for AuthClient {}

// TODO: this is basically POC that this approach will actually compile.
impl OpsCore {
    pub async fn sasl_auth_scram<'a, D>(
        &'a mut self,
        dispatcher: &'a mut D,
        opts: SASLAuthScramOptions,
        pipeline_cb: Option<impl (Fn()) + Send + Sync + 'static>,
    ) -> Result<SASLAuthScramPendingOp<Self, D>>
    where
        D: Dispatcher + Send + Sync,
    {
        let auth_client = AuthClient::new(opts.username(), opts.password());

        let mut client = SyncScramClient::<ScramSha256RustNative, AuthClient, AuthClient>::new(
            &auth_client,
            ScramNonce::None,
            scram_rs::ChannelBindType::None,
            &auth_client,
        )
        .unwrap();

        // This will only return an error when the client is already completed so safe to unwrap.
        let ci = client.init_client().encode_output_base64().unwrap();

        let req = SASLAuthRequest {
            payload: ci.into_bytes(),
            auth_mechanism: opts.hash.into(),
        };

        let op = self.sasl_auth(dispatcher, req).await?;

        if let Some(p_cb) = pipeline_cb {
            p_cb();
        }

        Ok(SASLAuthScramPendingOp::new(op, self, dispatcher))
    }
}
