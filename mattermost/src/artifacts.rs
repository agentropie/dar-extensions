//! Verified artifact delivery with a durable claim for each channel and thread.
use crate::api::MattermostClient;
use cap_chat::ArtifactReady;
use dar_extension_sdk::artifacts::{ArtifactDeliveryTarget, ArtifactStore, DeliveryClaimResult};
use std::io::Read;

pub async fn deliver(
    store: &ArtifactStore,
    client: &MattermostClient,
    artifacts: &[ArtifactReady],
    channel: &str,
    root: Option<&str>,
) -> anyhow::Result<()> {
    let target = ArtifactDeliveryTarget {
        surface_id: "mattermost".into(),
        origin_destination: serde_json::json!({"channel":channel,"root_id":root}).to_string(),
    };
    for artifact in artifacts {
        if artifact.validate(store).is_none() {
            continue;
        }
        let claim = match store.claim_delivery(artifact.id, target.clone()) {
            Ok(DeliveryClaimResult::Claimed(claim)) => claim,
            Ok(DeliveryClaimResult::Delivered(_) | DeliveryClaimResult::InProgress) => continue,
            Err(_) => {
                client
                    .create_post(channel, "Generated file could not be uploaded.", root, &[])
                    .await
                    .map_err(|_| anyhow::anyhow!("Mattermost artifact error delivery failed"))?;
                continue;
            }
        };
        let uploaded = async {
            let mut verified = store.open_verified(artifact.id)?;
            let filename = verified.metadata().filename.clone();
            let mut bytes = Vec::new();
            verified.read_to_end(&mut bytes)?;
            let files = client.upload_bytes(channel, &filename, bytes).await?;
            let ids = files.into_iter().map(|file| file.id).collect::<Vec<_>>();
            let post = client.create_post(channel, "", root, &ids).await?;
            Ok::<_, anyhow::Error>(post.id)
        }
        .await;
        match uploaded {
            Ok(remote_id) => {
                // Retain the claim if writing the receipt fails: the file was delivered.
                let _ = store.complete_delivery(claim, remote_id);
            }
            Err(_) => {
                let _ = store.release_delivery(claim);
                client
                    .create_post(channel, "Generated file could not be uploaded.", root, &[])
                    .await
                    .map_err(|_| anyhow::anyhow!("Mattermost artifact error delivery failed"))?;
            }
        }
    }
    Ok(())
}
