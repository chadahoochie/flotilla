use super::error::CosmosError;
use base64::prelude::*;
use hmac::{Hmac, Mac};
use sha2::Sha256;

type HmacSha256 = Hmac<Sha256>;

/// Azure Cosmos DB Master Key HMAC-SHA256 signature generator.
pub struct CosmosAuth;

impl CosmosAuth {
    /// Construct string-to-sign per Azure Cosmos DB REST specification:
    /// `"{verb}\n{resource_type}\n{resource_id}\n{date}\n\n"`
    pub fn build_string_to_sign(
        verb: &str,
        resource_type: &str,
        resource_id: &str,
        date: &str,
    ) -> String {
        format!(
            "{}\n{}\n{}\n{}\n\n",
            verb.to_lowercase(),
            resource_type.to_lowercase(),
            resource_id,
            date.to_lowercase(),
        )
    }

    /// Generate the full `authorization` header value for a Cosmos REST API request.
    pub fn generate_auth_header(
        verb: &str,
        resource_type: &str,
        resource_id: &str,
        date: &str,
        master_key_base64: &str,
    ) -> Result<String, CosmosError> {
        let key_bytes = BASE64_STANDARD
            .decode(master_key_base64.trim())
            .map_err(|e| CosmosError::Auth(format!("Invalid master key base64: {e}")))?;

        let string_to_sign = Self::build_string_to_sign(verb, resource_type, resource_id, date);

        let mut mac = HmacSha256::new_from_slice(&key_bytes)
            .map_err(|e| CosmosError::Auth(format!("HMAC init error: {e}")))?;
        mac.update(string_to_sign.as_bytes());
        let signature = BASE64_STANDARD.encode(mac.finalize().into_bytes());

        let url_encoded_sig = urlencoding::encode(&signature);
        Ok(format!("type=master&ver=1.0&sig={url_encoded_sig}"))
    }
}
