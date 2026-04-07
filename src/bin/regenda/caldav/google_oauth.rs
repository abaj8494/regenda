use anyhow::{Context, Result, bail};
use serde::{Deserialize, Serialize};
use std::path::PathBuf;

const GOOGLE_DEVICE_CODE_URL: &str = "https://oauth2.googleapis.com/device/code";
const GOOGLE_TOKEN_URL: &str = "https://oauth2.googleapis.com/token";
const GOOGLE_CALDAV_SCOPE: &str = "https://www.googleapis.com/auth/calendar.readonly";

const TOKEN_DIR: &str = "/opt/etc/reGenda";

/// Response from the device authorization request.
#[derive(Deserialize, Clone, Debug)]
pub struct DeviceAuthResponse {
    pub device_code: String,
    pub user_code: String,
    pub verification_url: String,
    pub expires_in: u64,
    pub interval: u64,
}

/// Response from the token exchange.
#[derive(Deserialize, Serialize, Clone, Debug)]
pub struct TokenResponse {
    pub access_token: String,
    pub token_type: String,
    pub expires_in: Option<u64>,
    pub refresh_token: Option<String>,
    pub scope: Option<String>,
}

/// Stored token (persisted to disk).
#[derive(Deserialize, Serialize, Clone, Debug)]
pub struct StoredToken {
    pub access_token: String,
    pub refresh_token: String,
    pub client_id: String,
    pub client_secret: String,
}

/// Error response from token polling.
#[derive(Deserialize, Debug)]
struct TokenErrorResponse {
    error: String,
}

/// Initiate the device authorization flow.
/// Returns the device auth response containing the user_code and verification_url
/// that must be shown to the user.
pub fn start_device_auth(client_id: &str) -> Result<DeviceAuthResponse> {
    let client = reqwest::blocking::Client::new();

    let resp = client
        .post(GOOGLE_DEVICE_CODE_URL)
        .form(&[
            ("client_id", client_id),
            ("scope", GOOGLE_CALDAV_SCOPE),
        ])
        .send()
        .context("Failed to start device authorization")?;

    if !resp.status().is_success() {
        let body = resp.text().unwrap_or_default();
        bail!("Device auth request failed: {}", body);
    }

    resp.json::<DeviceAuthResponse>()
        .context("Failed to parse device auth response")
}

/// Poll for the token after user has authorized.
/// Returns Ok(Some(token)) when authorized, Ok(None) when still pending,
/// or Err on fatal errors.
pub fn poll_for_token(
    client_id: &str,
    client_secret: &str,
    device_code: &str,
) -> Result<Option<TokenResponse>> {
    let client = reqwest::blocking::Client::new();

    let resp = client
        .post(GOOGLE_TOKEN_URL)
        .form(&[
            ("client_id", client_id),
            ("client_secret", client_secret),
            ("device_code", device_code),
            ("grant_type", "urn:ietf:params:oauth:grant-type:device_code"),
        ])
        .send()
        .context("Failed to poll for token")?;

    let status = resp.status();
    let body = resp.text().context("Failed to read token response")?;

    if status.is_success() {
        let token: TokenResponse =
            serde_json::from_str(&body).context("Failed to parse token response")?;
        return Ok(Some(token));
    }

    // Check if it's a pending/slow_down error (keep polling) or fatal
    if let Ok(error_resp) = serde_json::from_str::<TokenErrorResponse>(&body) {
        match error_resp.error.as_str() {
            "authorization_pending" | "slow_down" => return Ok(None),
            "expired_token" => bail!("Authorization expired. Please try again."),
            "access_denied" => bail!("Access denied by user."),
            other => bail!("Token error: {}", other),
        }
    }

    bail!("Unexpected token response ({}): {}", status, body);
}

/// Refresh an expired access token using the refresh token.
pub fn refresh_access_token(
    client_id: &str,
    client_secret: &str,
    refresh_token: &str,
) -> Result<String> {
    let client = reqwest::blocking::Client::new();

    let resp = client
        .post(GOOGLE_TOKEN_URL)
        .form(&[
            ("client_id", client_id),
            ("client_secret", client_secret),
            ("refresh_token", refresh_token),
            ("grant_type", "refresh_token"),
        ])
        .send()
        .context("Failed to refresh token")?;

    if !resp.status().is_success() {
        let body = resp.text().unwrap_or_default();
        bail!("Token refresh failed: {}", body);
    }

    let token: TokenResponse = resp
        .json()
        .context("Failed to parse refresh response")?;

    Ok(token.access_token)
}

/// Get the token file path for a given server name.
fn token_path(server_name: &str) -> PathBuf {
    PathBuf::from(TOKEN_DIR).join(format!("token_{}.json", server_name))
}

/// Load a stored token from disk.
pub fn load_stored_token(server_name: &str) -> Option<StoredToken> {
    let path = token_path(server_name);
    let data = std::fs::read_to_string(&path).ok()?;
    serde_json::from_str(&data).ok()
}

/// Save a token to disk.
pub fn save_stored_token(server_name: &str, token: &StoredToken) -> Result<()> {
    let path = token_path(server_name);

    // Ensure directory exists
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).ok();
    }

    let data = serde_json::to_string_pretty(token)
        .context("Failed to serialize token")?;
    std::fs::write(&path, data)
        .with_context(|| format!("Failed to write token to {:?}", path))?;

    Ok(())
}

/// Get a valid access token for a Google source.
/// First tries to use a stored refresh token. If none exists, returns None
/// to indicate that device auth flow is needed.
pub fn get_access_token(
    server_name: &str,
    client_id: &str,
    client_secret: &str,
) -> Result<Option<String>> {
    if let Some(stored) = load_stored_token(server_name) {
        // Try to refresh
        match refresh_access_token(client_id, client_secret, &stored.refresh_token) {
            Ok(access_token) => {
                // Update stored token with new access token
                let updated = StoredToken {
                    access_token: access_token.clone(),
                    ..stored
                };
                save_stored_token(server_name, &updated).ok();
                return Ok(Some(access_token));
            }
            Err(e) => {
                log::warn!(
                    "Failed to refresh token for {}: {:?}. Re-auth needed.",
                    server_name,
                    e
                );
                // Delete stale token
                let _ = std::fs::remove_file(token_path(server_name));
            }
        }
    }

    Ok(None)
}
