use std::{fmt, vec};
use std::error::Error;
use std::marker::PhantomData;
use std::str;

use base64::Engine;
use base64::engine::general_purpose;
use hmac::digest::KeyInit;
use hmac::Mac;
use rand::RngCore;
use sha2::Digest;

pub struct Client<D: Mac + KeyInit, H: Digest> {
    user: String,
    pass: String,
    client_nonce: Vec<u8>,
    server_nonce: Vec<u8>,
    salted_pass: Vec<u8>,
    auth_msg: Vec<u8>,
    hasher: PhantomData<H>,
    phantom: PhantomData<D>,
}

impl<D, H> Client<D, H>
where
    D: Mac + KeyInit,
    H: Digest,
{
    pub fn new(user: String, pass: String, client_nonce: impl Into<Option<Vec<u8>>>) -> Self {
        Client {
            user,
            pass,
            client_nonce: client_nonce.into().unwrap_or_default(),
            server_nonce: vec![],
            salted_pass: vec![],
            auth_msg: vec![],
            phantom: PhantomData,
            hasher: PhantomData,
        }
    }

    pub fn step1(&mut self) -> Result<Vec<u8>, ScramError> {
        if self.client_nonce.is_empty() {
            self.client_nonce = generate_nonce()?;
        }

        self.auth_msg.extend_from_slice(b"n=");
        self.auth_msg.extend_from_slice(self.user.as_bytes());
        self.auth_msg.extend_from_slice(b",r=");
        self.auth_msg.extend_from_slice(&self.client_nonce);

        let mut out = vec![];
        out.extend_from_slice(b"n,,");
        out.extend_from_slice(&self.auth_msg);

        Ok(out)
    }

    pub fn step2(&mut self, input: &[u8]) -> Result<Vec<u8>, ScramError> {
        self.auth_msg.push(b',');
        self.auth_msg.extend_from_slice(input);

        let fields: Vec<&[u8]> = input.split(|&b| b == b',').collect();
        if fields.len() != 3 {
            return Err(ScramError::new(format!(
                "Expected 3 fields in first SCRAM server message, got {}: {:?}",
                fields.len(),
                input
            )));
        }
        if !fields[0].starts_with(b"r=") {
            return Err(ScramError::new(format!(
                "Server sent an invalid SCRAM nonce: {:?}",
                fields[0]
            )));
        }
        if !fields[1].starts_with(b"s=") {
            return Err(ScramError::new(format!(
                "Server sent an invalid SCRAM salt: {:?}",
                fields[1]
            )));
        }
        if !fields[2].starts_with(b"i=") {
            return Err(ScramError::new(format!(
                "Server sent an invalid SCRAM iteration count: {:?}",
                fields[2]
            )));
        }

        fields[0][2..].clone_into(&mut self.server_nonce);
        if !self.server_nonce.starts_with(&self.client_nonce) {
            return Err(ScramError::new(format!(
                "Server SCRAM nonce is not prefixed by client nonce: got {:?}, want {:?}+\"...\"",
                self.server_nonce, self.client_nonce
            )));
        }

        let salt = general_purpose::STANDARD
            .decode(&fields[1][2..])
            .map_err(|e| {
                ScramError::new(format!("Cannot decode SCRAM salt sent by server: {:?}", e))
            })?;

        let iter_count = str::from_utf8(&fields[2][2..])
            .map_err(|e| {
                ScramError::new(format!(
                    "Server sent an invalid SCRAM iteration count: {:?}",
                    e
                ))
            })?
            .parse::<u32>()
            .map_err(|e| {
                ScramError::new(format!(
                    "Server sent an invalid SCRAM iteration count: {:?}",
                    e
                ))
            })?;
        self.salt_password(&salt, iter_count)?;

        self.auth_msg.extend_from_slice(b",c=biws,r=");
        self.auth_msg
            .extend_from_slice(self.server_nonce.as_slice());

        let mut out = vec![];
        out.extend_from_slice(b"c=biws,r=");
        out.extend_from_slice(self.server_nonce.as_slice());
        out.extend_from_slice(b",p=");
        out.extend_from_slice(self.client_proof()?.as_slice());

        Ok(out)
    }

    pub fn step3(&mut self, input: &[u8]) -> Result<(), ScramError> {
        let fields: Vec<&[u8]> = input.split(|&b| b == b',').collect();
        let isv = fields.len() == 1 && fields[0].starts_with(b"v=");
        let ise = fields.len() == 1 && fields[0].starts_with(b"e=");
        if ise {
            return Err(ScramError::new(format!(
                "SCRAM authentication error: {}",
                str::from_utf8(&fields[0][2..]).unwrap()
            )));
        } else if !isv {
            return Err(ScramError::new(format!(
                "Unsupported SCRAM final message from server: {:?}",
                input
            )));
        }

        let server_signature = self.server_signature()?;
        if server_signature != fields[0][2..] {
            return Err(ScramError::new(format!(
                "cannot authenticate SCRAM server signature: {:?}",
                &fields[0][2..]
            )));
        }
        Ok(())
    }

    fn salt_password(&mut self, salt: &[u8], iter_count: u32) -> Result<(), ScramError> {
        let mut mac = <D as Mac>::new_from_slice(self.pass.as_bytes())
            .map_err(|e| ScramError::new(e.to_string()))?;
        mac.update(salt);
        mac.update(&[0, 0, 0, 1]);
        let mut ui = mac.finalize().into_bytes().to_vec();
        let mut hi = ui.clone();

        for _ in 1..iter_count {
            let mut mac = <D as Mac>::new_from_slice(self.pass.as_bytes())
                .map_err(|e| ScramError::new(e.to_string()))?;
            mac.update(&ui);
            ui.copy_from_slice(&mac.finalize().into_bytes());
            for (i, b) in ui.iter().enumerate() {
                hi[i] ^= b;
            }
        }
        self.salted_pass = hi;
        Ok(())
    }

    fn server_signature(&self) -> Result<Vec<u8>, ScramError> {
        let mut mac = <D as Mac>::new_from_slice(self.salted_pass.as_slice())
            .map_err(|e| ScramError::new(e.to_string()))?;
        mac.update(b"Server Key");
        let server_key = mac.finalize().into_bytes().to_vec();
        mac =
            <D as Mac>::new_from_slice(&server_key).map_err(|e| ScramError::new(e.to_string()))?;
        mac.update(&self.auth_msg);
        let server_signature = mac.finalize().into_bytes().to_vec();
        let encoded = general_purpose::STANDARD.encode(server_signature);
        Ok(encoded.into_bytes())
    }

    fn client_proof(&self) -> Result<Vec<u8>, ScramError> {
        let mut mac = <D as Mac>::new_from_slice(self.salted_pass.as_ref())
            .map_err(|e| ScramError::new(e.to_string()))?;
        mac.update(b"Client Key");
        let client_key = mac.finalize().into_bytes().to_vec();

        let mut hash = H::new();
        hash.update(&client_key);
        let stored_key = hash.finalize();

        mac =
            <D as Mac>::new_from_slice(&stored_key).map_err(|e| ScramError::new(e.to_string()))?;
        mac.update(&self.auth_msg);

        let client_signature = mac.finalize().into_bytes().to_vec();

        let xor_result: Vec<u8> = client_key
            .iter()
            .zip(client_signature.iter())
            .map(|(a, b)| a ^ b)
            .collect();
        let encoded = general_purpose::STANDARD.encode(xor_result);
        Ok(encoded.into_bytes())
    }
}

#[derive(Debug)]
pub struct ScramError {
    message: String,
}

impl ScramError {
    fn new(message: String) -> Self {
        ScramError { message }
    }
}

impl fmt::Display for ScramError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.message)
    }
}

impl Error for ScramError {}

fn generate_nonce() -> Result<Vec<u8>, ScramError> {
    const NONCE_LEN: usize = 6;
    let mut buf = vec![0u8; NONCE_LEN];
    rand::thread_rng().fill_bytes(&mut buf);

    let mut target = vec![0; buf.len() * 4 / 3 + 4];

    let bytes_written = general_purpose::STANDARD
        .encode_slice(buf, &mut target)
        .map_err(|e| ScramError::new(e.to_string()))?;

    target.truncate(bytes_written);

    Ok(target)
}
