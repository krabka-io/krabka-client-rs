//! Parse a Kafka-style bootstrap string ("host:port,host:port") into
//! a list of resolved [`SocketAddr`](std::net::SocketAddr)s.

use std::{future::Future, net::SocketAddr};

use krabka_units::convert::TimeExt as _;

use crate::{
    connection::{ClientDnsLookup, ClientDnsTimeout},
    error::ClientError,
    security::connection_target_host,
};

pub(crate) async fn bounded_lookup<F>(
    timeout: ClientDnsTimeout,
    lookup: F,
) -> Result<F::Output, tokio::time::error::Elapsed>
where
    F: Future,
{
    tokio::time::timeout(timeout.time().to_std(), lookup).await
}

/// Parse a comma-separated `host:port` list and resolve each entry with
/// [`tokio::net::lookup_host`].
///
/// This function silently skips entries that fail to resolve. It returns
/// [`ClientError::Disconnected`] if *none* resolve.
#[tracing::instrument(level = "debug", skip_all, fields(bootstrap = %bootstrap), err)]
#[cfg(test)]
pub async fn resolve(
    bootstrap: &str,
    dns_timeout: ClientDnsTimeout,
) -> Result<Vec<SocketAddr>, ClientError> {
    resolve_with_server_names(bootstrap, dns_timeout, ClientDnsLookup::UseAllDnsIps)
        .await
        .map(|addresses| addresses.into_iter().map(|(address, _)| address).collect())
}

pub(crate) async fn resolve_with_server_names(
    bootstrap: &str,
    dns_timeout: ClientDnsTimeout,
    lookup: ClientDnsLookup,
) -> Result<Vec<(SocketAddr, String)>, ClientError> {
    let mut out = Vec::new();
    for part in bootstrap.split(',') {
        let part = part.trim();
        if part.is_empty() {
            continue;
        }
        match bounded_lookup(dns_timeout, tokio::net::lookup_host(part)).await {
            Ok(Ok(iter)) => match lookup {
                ClientDnsLookup::UseAllDnsIps => out
                    .extend(iter.map(|address| (address, connection_target_host(part).to_owned()))),
                ClientDnsLookup::ResolveCanonicalBootstrapServersOnly => {
                    out.extend(canonical_addresses(part, iter, dns_timeout).await);
                }
            },
            Ok(Err(error)) => {
                tracing::warn!(part, error = %error, "bootstrap resolve failed");
            }
            Err(error) => {
                tracing::warn!(part, error = %error, "bootstrap resolve timed out");
            }
        }
    }
    if out.is_empty() {
        return Err(ClientError::Disconnected);
    }
    Ok(out)
}

/// Replace each resolved bootstrap address with the canonical host name of
/// its reverse lookup, and the address of that name, as Kafka's
/// `ClientUtils.resolve` does for `resolve_canonical_bootstrap_servers_only`.
/// An address whose canonical name does not resolve is skipped.
async fn canonical_addresses(
    part: &str,
    addresses: impl Iterator<Item = SocketAddr>,
    dns_timeout: ClientDnsTimeout,
) -> Vec<(SocketAddr, String)> {
    let mut out = Vec::new();
    for address in addresses {
        let ip = address.ip();
        let reverse = tokio::task::spawn_blocking(move || dns_lookup::lookup_addr(&ip));
        // Java's `getCanonicalHostName` gives the address text when the
        // reverse lookup fails.
        let canonical = match bounded_lookup(dns_timeout, reverse).await {
            Ok(Ok(Ok(name))) => name,
            _ => ip.to_string(),
        };
        let resolved = bounded_lookup(
            dns_timeout,
            tokio::net::lookup_host((canonical.clone(), address.port())),
        )
        .await
        .ok()
        .and_then(Result::ok)
        .and_then(|mut resolved| resolved.next());
        if let Some(resolved) = resolved {
            out.push((resolved, canonical));
        } else {
            tracing::warn!(
                part,
                canonical = %canonical,
                "bootstrap canonical host name did not resolve"
            );
        }
    }
    out
}

/// Keep the first address and the later addresses of the same family, as
/// Kafka's `ClientUtils.filterPreferredAddresses` does.
pub(crate) fn filter_preferred_addresses(
    addresses: impl IntoIterator<Item = SocketAddr>,
) -> Vec<SocketAddr> {
    let mut addresses = addresses.into_iter();
    let Some(first) = addresses.next() else {
        return Vec::new();
    };
    std::iter::once(first)
        .chain(addresses.filter(|address| address.is_ipv4() == first.is_ipv4()))
        .collect()
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use assert2::assert;
    use krabka_units::millis;

    use super::*;
    use crate::connection::ClientDnsTimeout;

    #[tokio::test]
    async fn resolve_returns_addresses_for_valid_entries() {
        let addrs = resolve(
            "127.0.0.1:9092, 127.0.0.1:9093",
            ClientDnsTimeout::default(),
        )
        .await
        .expect("literal addresses resolve");

        assert!(addrs.len() == 2);
        assert!(addrs.iter().any(|addr| addr.port() == 9092));
        assert!(addrs.iter().any(|addr| addr.port() == 9093));
    }

    #[tokio::test]
    async fn resolve_retains_tls_names_before_dns() {
        let addresses = resolve_with_server_names(
            "localhost:9092,[::1]:9093",
            ClientDnsTimeout::default(),
            ClientDnsLookup::UseAllDnsIps,
        )
        .await
        .expect("addresses resolve");

        assert!(
            addresses
                .iter()
                .any(|(address, name)| address.port() == 9092 && name == "localhost")
        );
        assert!(
            addresses
                .iter()
                .any(|(address, name)| address.port() == 9093 && name == "::1")
        );
    }

    #[tokio::test]
    async fn resolve_errors_when_no_entries_resolve() {
        let err = resolve(" , ", ClientDnsTimeout::default())
            .await
            .expect_err("empty entries do not resolve");

        assert!(matches!(err, ClientError::Disconnected));
    }

    #[tokio::test(start_paused = true)]
    async fn bounded_lookup_stops_at_the_configured_deadline() {
        let timeout = ClientDnsTimeout::new(millis(37)).expect("positive timeout");
        let started = tokio::time::Instant::now();
        let result = bounded_lookup(timeout, std::future::pending::<()>()).await;
        assert!(result.is_err());
        assert!(started.elapsed() == Duration::from_millis(37));
    }

    #[test]
    fn preferred_addresses_keep_the_family_of_the_first_address() {
        let v4 = |port| SocketAddr::from(([10, 0, 0, 1], port));
        let v6 = |port| SocketAddr::from(([0, 0, 0, 0, 0, 0, 0, 1], port));
        for (name, input, expected) in [
            ("empty", vec![], vec![]),
            ("IPv4 first", vec![v4(1), v6(2), v4(3)], vec![v4(1), v4(3)]),
            ("IPv6 first", vec![v6(1), v4(2), v6(3)], vec![v6(1), v6(3)]),
        ] {
            assert2::check!(filter_preferred_addresses(input) == expected, "{name}");
        }
    }

    #[tokio::test]
    async fn canonical_lookup_names_each_bootstrap_address_by_its_reverse_lookup() {
        let addresses = resolve_with_server_names(
            "127.0.0.1:9092",
            ClientDnsTimeout::default(),
            ClientDnsLookup::ResolveCanonicalBootstrapServersOnly,
        )
        .await
        .expect("loopback resolves");
        let expected_name = dns_lookup::lookup_addr(&"127.0.0.1".parse().unwrap())
            .unwrap_or_else(|_| "127.0.0.1".to_owned());
        assert!(addresses.len() == 1);
        assert!(addresses[0].0.port() == 9092);
        assert!(addresses[0].1 == expected_name);
    }

    #[tokio::test]
    async fn resolve_skips_a_failed_entry_and_keeps_later_addresses() {
        let addrs = resolve(":,127.0.0.1:9093", ClientDnsTimeout::default())
            .await
            .expect("later address resolves");
        assert!(addrs.iter().any(|addr| addr.port() == 9093));
    }
}
