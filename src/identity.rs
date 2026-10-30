//! Workload identity, detected rather than configured.
//!
//! # Why there are no flags here
//!
//! Every other knob in this canary is a `--flag` in [`crate::cli`]. This one is
//! not, and the asymmetry is deliberate: the values below are not decisions an
//! operator makes about *this* canary, they are facts about the platform it
//! landed on. On AKS the `azure-wi-webhook` writes them into the pod. On EKS the
//! IRSA webhook writes the same shape under different names. Here a
//! spiffe-helper sidecar writes the token and a ConfigMap supplies the other
//! two. In all three cases something outside the process already knows the
//! answer, and a flag would only be a second place for it to be wrong.
//!
//! So the rule is: **if the environment describes a workload identity, use it.**
//! A deployment that wants OAUTHBEARER adds no canary configuration at all — it
//! adds the identity, and this notices. That is the same contract
//! `DefaultAzureCredential` implements, and it is why a binary built against it
//! moves between clusters without a rebuild or a new argument.
//!
//! # What is derived rather than asked for
//!
//! * the token endpoint — from the authority host and tenant
//! * the scope — `api://<client-id>/.default`, which is what Entra wants from a
//!   client asking for a token about itself
//!
//! Both are overridable, but neither has to be set for the common case, and
//! "the common case needs nothing" is the whole design.
//!
//! # Partial configuration is an error
//!
//! Three variables, and any strict subset of them is a mistake rather than a
//! preference — a ConfigMap that lost a key, a webhook that did not fire. The
//! alternative is to treat "two of three" as "identity off", which produces a
//! canary that silently connects anonymously and reports on a listener nobody
//! meant to test. It fails at startup instead, naming what is missing.

use std::env;
use std::path::PathBuf;

use anyhow::{Result, bail};
use kafka_conn::SaslConfig;

use crate::token::FederatedTokenProvider;

/// The application (client) id. Not the service principal's object id — that
/// one is what the broker sees as the principal, and it never appears here.
const CLIENT_ID: &str = "AZURE_CLIENT_ID";
/// The directory (tenant) id, which selects the issuer.
const TENANT_ID: &str = "AZURE_TENANT_ID";
/// Path to the assertion. Written by a sidecar here, by the kubelet on AKS.
const TOKEN_FILE: &str = "AZURE_FEDERATED_TOKEN_FILE";
/// Overridable for sovereign and government clouds, which have their own hosts.
const AUTHORITY_HOST: &str = "AZURE_AUTHORITY_HOST";
/// An escape hatch for a broker that wants a token about some other resource.
const SCOPE: &str = "AZURE_KAFKA_SCOPE";

/// Where the public cloud's token endpoints live.
const DEFAULT_AUTHORITY: &str = "https://login.microsoftonline.com/";

/// A federated identity, as described by the environment.
#[derive(Debug, Clone)]
pub struct WorkloadIdentity {
    client_id: String,
    tenant_id: String,
    token_file: PathBuf,
    authority: String,
    scope: String,
}

impl WorkloadIdentity {
    /// Read the environment.
    ///
    /// `Ok(None)` means no workload identity is configured, which is a valid
    /// deployment — an unauthenticated listener is what this canary has always
    /// connected to. `Err` means one was half-configured.
    pub fn from_env() -> Result<Option<Self>> {
        let client_id = present(CLIENT_ID);
        let tenant_id = present(TENANT_ID);
        let token_file = present(TOKEN_FILE);

        match (client_id, tenant_id, token_file) {
            (None, None, None) => Ok(None),
            (Some(client_id), Some(tenant_id), Some(token_file)) => {
                let authority =
                    present(AUTHORITY_HOST).unwrap_or_else(|| DEFAULT_AUTHORITY.to_owned());
                // `<client-id>/.default` is Entra's way of saying "a token for
                // this application, with whatever it has been granted". The
                // `api://` form is the Application ID URI, which the app
                // registration has to expose — without it the exchange fails at
                // the issuer with `AADSTS500011`, naming a resource principal
                // rather than the missing URI.
                let scope = present(SCOPE).unwrap_or_else(|| format!("api://{client_id}/.default"));
                Ok(Some(Self {
                    client_id,
                    tenant_id,
                    token_file: PathBuf::from(token_file),
                    authority,
                    scope,
                }))
            }
            (client_id, tenant_id, token_file) => {
                let missing: Vec<&str> = [
                    (CLIENT_ID, client_id.is_none()),
                    (TENANT_ID, tenant_id.is_none()),
                    (TOKEN_FILE, token_file.is_none()),
                ]
                .into_iter()
                .filter_map(|(name, absent)| absent.then_some(name))
                .collect();
                bail!(
                    "workload identity is half-configured: {} not set. \
                     Set all three or none — connecting anonymously because one \
                     of them is missing would test a listener nobody meant to test.",
                    missing.join(", ")
                )
            }
        }
    }

    /// The OIDC token endpoint for this tenant.
    ///
    /// Built rather than configured. The authority is joined without doubling
    /// the separator because `AZURE_AUTHORITY_HOST` conventionally carries a
    /// trailing slash and an operator setting it by hand conventionally does
    /// not — and `//` in the path is a 404 that names nothing.
    fn token_endpoint(&self) -> String {
        format!(
            "{}/{}/oauth2/v2.0/token",
            self.authority.trim_end_matches('/'),
            self.tenant_id
        )
    }

    /// The SASL configuration this identity implies.
    ///
    /// One provider for the whole client: it holds the cached token, so sharing
    /// it is what keeps every connection on one token and one fetch rather than
    /// one fetch per broker.
    pub fn sasl(&self) -> Result<SaslConfig> {
        let provider = FederatedTokenProvider::new(
            &self.token_endpoint(),
            &self.client_id,
            &self.scope,
            // Re-read per fetch, not captured now. The sidecar rewrites this
            // file every few minutes and only the newest bytes verify.
            &self.token_file,
        )?;

        Ok(SaslConfig::oauth_bearer(provider))
    }

    /// What to log at startup, with nothing secret in it.
    ///
    /// The client id and tenant are identifiers, not credentials; the assertion
    /// path is worth printing because a token file that is not there is the
    /// single most likely thing to be wrong.
    pub fn describe(&self) -> String {
        format!(
            "client_id={} tenant={} assertion={} scope={}",
            self.client_id,
            self.tenant_id,
            self.token_file.display(),
            self.scope
        )
    }
}

/// An environment variable that is set and not blank.
///
/// Blank counts as absent: a ConfigMap key with an empty value is a key someone
/// meant to fill in, and treating it as a real client id defers the error to
/// the issuer, which answers with `invalid_client` and says nothing about which
/// of the several possible fields was empty.
fn present(name: &str) -> Option<String> {
    env::var(name)
        .ok()
        .map(|value| value.trim().to_owned())
        .filter(|value| !value.is_empty())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn identity() -> WorkloadIdentity {
        WorkloadIdentity {
            client_id: "app-id".to_owned(),
            tenant_id: "tenant-id".to_owned(),
            token_file: PathBuf::from("/var/run/spiffe/svid/azure-token"),
            authority: DEFAULT_AUTHORITY.to_owned(),
            scope: "api://app-id/.default".to_owned(),
        }
    }

    #[test]
    fn the_token_endpoint_survives_an_authority_with_or_without_a_slash() {
        let expected = "https://login.microsoftonline.com/tenant-id/oauth2/v2.0/token";

        assert_eq!(identity().token_endpoint(), expected);

        let mut bare = identity();
        bare.authority = "https://login.microsoftonline.com".to_owned();
        assert_eq!(bare.token_endpoint(), expected);
    }

    #[test]
    fn describe_carries_the_assertion_path_but_never_its_contents() {
        let rendered = identity().describe();
        assert!(
            rendered.contains("/var/run/spiffe/svid/azure-token"),
            "{rendered}"
        );
        assert!(rendered.contains("client_id=app-id"), "{rendered}");
    }

    #[test]
    fn a_federated_identity_builds_a_sasl_config() {
        assert!(identity().sasl().is_ok());
    }
}
