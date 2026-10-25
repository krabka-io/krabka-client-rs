use std::error::Error;

use krabka_client_admin::{
    AdminClient, AdminClientLike as _, CreateTopicSpec, TopicMutationOptions,
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    println!("Connecting to Krabka broker at 127.0.0.1:9092...");
    let mut admin = AdminClient::connect(&["127.0.0.1:9092".to_owned()]).await?;
    println!("Connected successfully! Creating topic 'test-topic'...");

    admin
        .create_topics(
            &[CreateTopicSpec::builder()
                .name("test-topic")
                .partitions(1)
                .replicas(1)
                .build()],
            TopicMutationOptions::with_timeout(krabka_units::secs(30)),
        )
        .await?;
    println!("Topic 'test-topic' created successfully! Fetching metadata...");

    let metadata = admin.metadata(&["test-topic"]).await?;
    println!("Fetched metadata for 'test-topic': {:#?}", metadata.topics);

    Ok(())
}
