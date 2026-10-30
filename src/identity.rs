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
//!
//! # The schema registry is opt-in, and asked for rather than derived
//!
//! `AZURE_SCHEMA_REGISTRY_SCOPE` is the one value here with no default. The
//! Kafka scope can be derived because the broker accepts a token about this
//! application itself; a registry that enforces authentication wants a token
//! about *its own* app registration, and nothing in the environment says which
//! one that is. Unset means the registry is called anonymously, as it always
//! has been — so the variable can ship ahead of any registry that requires it.
//!
//! Set without an identity it is a mistake of the same kind as a half-set
//! identity, and fails the same way: a registry scope is a request for a token,
//! and with no identity there is nothing to ask with.

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
/// The registry's own scope, when it requires a token. See the module docs for
/// why this one has no default.
const REGISTRY_SCOPE: &str = "AZURE_SCHEMA_REGISTRY_SCOPE";

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
    registry_scope: Option<String>,
}

impl WorkloadIdentity {
    /// Read the environment.
    ///
    /// `Ok(None)` means no workload identity is configured, which is a valid
    /// deployment — an unauthenticated listener is what this canary has always
    /// connected to. `Err` means one was half-configured.
    pub fn from_env() -> Result<Option<Self>> {
        Self::from_vars(present)
    }

    /// The rules of [`Self::from_env`], over any source of variables.
    ///
    /// Split out so the rules can be tested without `std::env::set_var`, which
    /// is process-global — and `unsafe` in edition 2024 for exactly that
    /// reason — so tests setting it would race each other. `var` must already
    /// treat blank as absent, as [`present`] does.
    fn from_vars(var: impl Fn(&str) -> Option<String>) -> Result<Option<Self>> {
        let client_id = var(CLIENT_ID);
        let tenant_id = var(TENANT_ID);
        let token_file = var(TOKEN_FILE);
        let registry_scope = var(REGISTRY_SCOPE);

        match (client_id, tenant_id, token_file) {
            (None, None, None) => {
                if registry_scope.is_some() {
                    bail!(
                        "{REGISTRY_SCOPE} is set but no workload identity is: a registry \
                         scope asks for a token, and without {CLIENT_ID}, {TENANT_ID} and \
                         {TOKEN_FILE} there is nothing to ask with. Set the identity, or \
                         unset {REGISTRY_SCOPE} to call the registry anonymously."
                    );
                }
                Ok(None)
            }
            (Some(client_id), Some(tenant_id), Some(token_file)) => {
                let authority = var(AUTHORITY_HOST).unwrap_or_else(|| DEFAULT_AUTHORITY.to_owned());
                // `<client-id>/.default` is Entra's way of saying "a token for
                // this application, with whatever it has been granted". The
                // `api://` form is the Application ID URI, which the app
                // registration has to expose — without it the exchange fails at
                // the issuer with `AADSTS500011`, naming a resource principal
                // rather than the missing URI.
                let scope = var(SCOPE).unwrap_or_else(|| format!("api://{client_id}/.default"));
                Ok(Some(Self {
                    client_id,
                    tenant_id,
                    token_file: PathBuf::from(token_file),
                    authority,
                    scope,
                    registry_scope,
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

    /// A token source for the schema registry, or `None` when it is called
    /// anonymously.
    ///
    /// A provider rather than a token, unlike the producer's: this canary
    /// resolves schema ids for as long as it runs, so whatever it was handed at
    /// startup expires long before it stops — see [`crate::registry`]. Same
    /// client id and assertion as the Kafka connection, a different resource,
    /// and a separate cache, since the two tokens are about different things.
    pub fn registry_tokens(&self) -> Result<Option<FederatedTokenProvider>> {
        let Some(scope) = &self.registry_scope else {
            return Ok(None);
        };
        Ok(Some(FederatedTokenProvider::new(
            &self.token_endpoint(),
            &self.client_id,
            scope,
            &self.token_file,
        )?))
    }

    /// What to log at startup, with nothing secret in it.
    ///
    /// The client id and tenant are identifiers, not credentials; the assertion
    /// path is worth printing because a token file that is not there is the
    /// single most likely thing to be wrong.
    pub fn describe(&self) -> String {
        format!(
            "client_id={} tenant={} assertion={} scope={} registry_scope={}",
            self.client_id,
            self.tenant_id,
            self.token_file.display(),
            self.scope,
            self.registry_scope.as_deref().unwrap_or("none")
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
            registry_scope: None,
        }
    }

    /// A variable source holding exactly `vars`, as [`present`] would see it.
    fn vars(vars: &[(&str, &str)]) -> impl Fn(&str) -> Option<String> {
        let vars: Vec<(String, String)> = vars
            .iter()
            .map(|(k, v)| ((*k).to_owned(), (*v).to_owned()))
            .collect();
        move |name| vars.iter().find(|(k, _)| k == name).map(|(_, v)| v.clone())
    }

    const IDENTITY: [(&str, &str); 3] = [
        (CLIENT_ID, "app-id"),
        (TENANT_ID, "tenant-id"),
        (TOKEN_FILE, "/var/run/spiffe/svid/azure-token"),
    ];

    #[test]
    fn no_registry_scope_means_an_anonymous_registry() {
        let identity = WorkloadIdentity::from_vars(vars(&IDENTITY))
            .unwrap()
            .unwrap();
        assert_eq!(identity.registry_scope, None);
    }

    #[test]
    fn a_registry_scope_rides_along_with_the_identity() {
        let mut with_scope = IDENTITY.to_vec();
        with_scope.push((REGISTRY_SCOPE, "registry-id/.default"));

        let identity = WorkloadIdentity::from_vars(vars(&with_scope))
            .unwrap()
            .unwrap();
        assert_eq!(
            identity.registry_scope.as_deref(),
            Some("registry-id/.default")
        );
        // The Kafka scope is not affected by it.
        assert_eq!(identity.scope, "api://app-id/.default");
    }

    #[test]
    fn a_registry_scope_without_an_identity_is_refused() {
        let err = WorkloadIdentity::from_vars(vars(&[(REGISTRY_SCOPE, "registry-id/.default")]))
            .unwrap_err();
        assert!(format!("{err}").contains(REGISTRY_SCOPE), "{err}");
    }

    #[test]
    fn nothing_set_is_still_no_identity() {
        assert!(WorkloadIdentity::from_vars(vars(&[])).unwrap().is_none());
    }

    #[test]
    fn an_anonymous_registry_has_no_token_source() {
        assert!(identity().registry_tokens().unwrap().is_none());
    }

    #[test]
    fn a_registry_scope_builds_a_token_source() {
        let mut with_scope = identity();
        with_scope.registry_scope = Some("registry-id/.default".to_owned());
        // Builds without fetching: the first token is asked for later.
        assert!(with_scope.registry_tokens().unwrap().is_some());
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
