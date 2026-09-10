//! Broker registration administration.

use krabka_protocol::owned::unregister_broker_request::UnregisterBrokerRequest;

use crate::{AdminClient, AdminError, NOT_CONTROLLER, kafka_error_name};

impl AdminClient {
    /// Unregisters one broker from the active `KRaft` controller.
    ///
    /// # Errors
    /// Returns a transport, protocol, or broker error. A stale-controller
    /// response is retried once after controller discovery.
    pub async fn unregister_broker(&mut self, broker_id: i32) -> Result<(), AdminError> {
        let request = UnregisterBrokerRequest {
            broker_id,
            ..Default::default()
        };
        let first = self.conn.send(request.clone()).await?;
        if first.error_code != NOT_CONTROLLER {
            return unregister_error(first.error_code, first.error_message);
        }
        self.refresh_controller_connection().await?;
        let second = self.conn.send(request).await?;
        if second.error_code == NOT_CONTROLLER {
            return Err(AdminError::NotControllerExhausted);
        }
        unregister_error(second.error_code, second.error_message)
    }
}

fn unregister_error(code: i16, message: Option<String>) -> Result<(), AdminError> {
    if code == 0 {
        Ok(())
    } else {
        Err(AdminError::Broker {
            api: "UnregisterBroker",
            code,
            name: kafka_error_name(code),
            message,
        })
    }
}

#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::*;

    #[test]
    fn unregister_success_and_error_are_distinct() {
        assert!(unregister_error(0, None).is_ok());
        assert!(matches!(
            unregister_error(42, Some("stale".into())),
            Err(AdminError::Broker {
                api: "UnregisterBroker",
                code: 42,
                ..
            })
        ));
    }
}
