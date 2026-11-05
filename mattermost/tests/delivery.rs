pub use mattermost::{api, config};
#[path = "../src/test_http.rs"]
mod test_http;

mod attachments_tests {
    use crate::{
        config::SecretToken,
        test_http::{Response, Server},
    };
    use mattermost::api::MattermostClient;
    use mattermost::attachments::*;
    #[tokio::test]
    async fn attachments_are_downloaded_and_prompt_contains_only_local_metadata() {
        let mut server = Server::start(vec![
            Response::json(serde_json::json!({"id":"ffffffffffffffffffffffffff","name":"notes.txt","mime_type":"text/plain","size":5})),
            Response::bytes(b"hello".to_vec()),
        ]).await;
        let client = MattermostClient::new(
            &server.base,
            SecretToken::new("invented-secret-token".into()),
        )
        .unwrap();
        let root = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("target/test-uploads")
            .join(format!(
                "{}-{}",
                std::process::id(),
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_nanos()
            ));
        tokio::fs::create_dir_all(&root).await.unwrap();
        let result = prompt(
            &client,
            &root,
            &["ffffffffffffffffffffffffff".into()],
            "Read this".into(),
        )
        .await
        .unwrap();
        let metadata: serde_json::Value = serde_json::from_str(
            result
                .split("Attachment metadata (untrusted data): ")
                .nth(1)
                .expect("metadata"),
        )
        .unwrap();
        assert_eq!(metadata["name"], "notes.txt");
        assert_eq!(metadata["mime"], "text/plain");
        assert_eq!(metadata["source"], "untrusted Mattermost attachment");
        let path = metadata["path"].as_str().unwrap();
        assert!(path.starts_with("data/uploads/"));
        assert_eq!(tokio::fs::read(root.join(path)).await.unwrap(), b"hello");
        for private in [
            "invented-secret-token",
            "ffffffffffffffffffffffffff",
            server.base.as_str(),
        ] {
            assert!(!result.contains(private));
        }
        assert_eq!(
            server.next().await.path,
            "/api/v4/files/ffffffffffffffffffffffffff/info"
        );
        let download = server.next().await;
        assert_eq!(download.path, "/api/v4/files/ffffffffffffffffffffffffff");
        assert!(download
            .headers
            .to_ascii_lowercase()
            .contains("authorization: bearer invented-secret-token"));
        server.finish().await;
        tokio::fs::remove_dir_all(root).await.unwrap();
    }
    #[tokio::test]
    async fn size_limit_applies_to_metadata_headers_and_streamed_bytes() {
        for source in ["metadata", "header", "stream"] {
            let declared = if source == "metadata" {
                26 * 1024 * 1024
            } else {
                1
            };
            let mut responses = vec![Response::json(
                serde_json::json!({"id":"gggggggggggggggggggggggggg","name":"notes.txt","mime_type":"text/plain","size":declared}),
            )];
            if source == "header" {
                let mut response = Response::bytes(Vec::new());
                response
                    .headers
                    .push(("Content-Length".into(), "27262976".into()));
                responses.push(response);
            } else if source == "stream" {
                let mut body = b"1a00000\r\n".to_vec();
                body.resize(body.len() + 26 * 1024 * 1024, b'x');
                body.extend_from_slice(b"\r\n0\r\n\r\n");
                let mut response = Response::bytes(body);
                response
                    .headers
                    .push(("Transfer-Encoding".into(), "chunked".into()));
                responses.push(response);
            }
            let server = Server::start(responses).await;
            let client =
                MattermostClient::new(&server.base, SecretToken::new("invented-token".into()))
                    .unwrap();
            let root = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(format!(
                "target/test-upload-limit-{source}-{}",
                std::process::id()
            ));
            tokio::fs::create_dir_all(&root).await.unwrap();
            let error = prompt(
                &client,
                &root,
                &["gggggggggggggggggggggggggg".into()],
                "Read".into(),
            )
            .await
            .expect_err(source);
            assert!(error.to_string().contains("25 MiB"), "{source}: {error}");
            if let Ok(mut entries) = tokio::fs::read_dir(root.join("data/uploads")).await {
                assert!(entries.next_entry().await.unwrap().is_none());
            }
            server.finish().await;
            tokio::fs::remove_dir_all(root).await.unwrap();
        }
    }

    #[tokio::test]
    async fn unsafe_filenames_fail_before_download() {
        for name in [
            "../secret",
            "/secret",
            "sub/file",
            "sub\\file",
            ".",
            "..",
            "bad\nname",
        ] {
            let server = Server::start(vec![Response::json(
                serde_json::json!({"id":"gggggggggggggggggggggggggg","name":name,"size":1}),
            )])
            .await;
            let client =
                MattermostClient::new(&server.base, SecretToken::new("invented-token".into()))
                    .unwrap();
            let root = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
                .join(format!("target/test-upload-unsafe-{}", std::process::id()));
            tokio::fs::create_dir_all(&root).await.unwrap();
            let error = prompt(
                &client,
                &root,
                &["gggggggggggggggggggggggggg".into()],
                "Read".into(),
            )
            .await
            .unwrap_err();
            assert!(
                error.to_string().contains("filename is unsafe"),
                "{name:?}: {error}"
            );
            server.finish().await;
            tokio::fs::remove_dir_all(root).await.unwrap();
        }
    }

    #[tokio::test]
    async fn upload_directory_symlinks_cannot_escape_the_agent_root() {
        let server = Server::start(vec![Response::json(
            serde_json::json!({"id":"gggggggggggggggggggggggggg","name":"notes.txt","size":1}),
        )])
        .await;
        let client =
            MattermostClient::new(&server.base, SecretToken::new("invented-token".into())).unwrap();
        let base = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join(format!("target/test-upload-symlink-{}", std::process::id()));
        let root = base.join("agent");
        let outside = base.join("outside");
        tokio::fs::create_dir_all(&root).await.unwrap();
        tokio::fs::create_dir_all(&outside).await.unwrap();
        std::os::unix::fs::symlink(&outside, root.join("data")).unwrap();
        let result = prompt(
            &client,
            &root,
            &["gggggggggggggggggggggggggg".into()],
            "Read".into(),
        )
        .await;
        assert!(result.is_err(), "symlink must be rejected");
        assert!(
            !outside.join("uploads").exists(),
            "must not create directories outside the root"
        );
        server.finish().await;
        tokio::fs::remove_dir_all(base).await.unwrap();
    }
    #[tokio::test]
    async fn cancelling_a_pending_attachment_download_removes_its_partial_file() {
        use tokio::{
            io::{AsyncReadExt, AsyncWriteExt},
            net::TcpListener,
            sync::oneshot,
        };
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base = format!("http://{}", listener.local_addr().unwrap());
        let (release_tx, release_rx) = oneshot::channel();
        let server = tokio::spawn(async move {
            let info = r#"{"id":"gggggggggggggggggggggggggg","name":"notes.txt","mime_type":"text/plain","size":1024}"#;
            for is_download in [false, true] {
                let (mut socket, _) = listener.accept().await.unwrap();
                let mut request = Vec::new();
                while !request.windows(4).any(|bytes| bytes == b"\r\n\r\n") {
                    let mut buffer = [0; 1024];
                    let count = socket.read(&mut buffer).await.unwrap();
                    assert!(count > 0);
                    request.extend_from_slice(&buffer[..count]);
                }
                if is_download {
                    socket.write_all(b"HTTP/1.1 200 OK\r\nConnection: close\r\nTransfer-Encoding: chunked\r\n\r\n5\r\nhello\r\n").await.unwrap();
                    let _ = release_rx.await;
                    break;
                }
                socket.write_all(format!("HTTP/1.1 200 OK\r\nConnection: close\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{info}",info.len()).as_bytes()).await.unwrap();
            }
        });
        let client =
            MattermostClient::new(&base, SecretToken::new("invented-token".into())).unwrap();
        let root = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join(format!("target/test-upload-cancel-{}", std::process::id()));
        tokio::fs::create_dir_all(&root).await.unwrap();
        let task_root = root.clone();
        let task = tokio::spawn(async move {
            prompt(
                &client,
                &task_root,
                &["gggggggggggggggggggggggggg".into()],
                "Read".into(),
            )
            .await
        });
        let uploads = root.join("data/uploads");
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            loop {
                if let Ok(mut entries) = tokio::fs::read_dir(&uploads).await {
                    while let Some(entry) = entries.next_entry().await.unwrap() {
                        if entry.metadata().await.unwrap().len() > 0 {
                            return;
                        }
                    }
                }
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("download should write its first bytes while the server keeps streaming");
        task.abort();
        assert!(task.await.unwrap_err().is_cancelled());
        let mut entries = tokio::fs::read_dir(&uploads).await.unwrap();
        let empty = entries.next_entry().await.unwrap().is_none();
        let _ = release_tx.send(());
        server.await.unwrap();
        tokio::fs::remove_dir_all(root).await.unwrap();
        assert!(
            empty,
            "cancelling the public download must remove partial uploads"
        );
    }
}

mod tools_tests {
    use crate::{
        config::SecretToken,
        test_http::{Response, Server},
    };
    use dar_extension_sdk::tools::ToolExecutor;
    use mattermost::tools::*;
    use mattermost::{api::MattermostClient, config::MattermostConfig};
    use serde_json::{json, Value};
    use std::sync::Arc;
    fn tool(base: &str, config: MattermostConfig, kind: ToolKind) -> Arc<MattermostTool> {
        MattermostTool::new(
            MattermostClient::new(base, SecretToken::new("invented-token".into())).unwrap(),
            config,
            kind,
        )
    }
    #[tokio::test]
    async fn send_requires_text_and_exactly_one_destination() {
        let tool = tool(
            "http://127.0.0.1:9",
            MattermostConfig::default(),
            ToolKind::Send,
        );
        for args in [
            json!({}),
            json!({"text":" " ,"channel":"room"}),
            json!({"text":"hello"}),
            json!({"text":"hello","channel":"room","user":"sage"}),
        ] {
            let outcome = tool.execute(args).await.unwrap();
            assert!(outcome.is_error);
            assert_eq!(outcome.error.unwrap().code, "invalid_args");
        }
    }
    #[tokio::test]
    async fn send_resolves_a_channel_id_and_preserves_thread_markdown() {
        let mut server = Server::start(vec![
            Response::json(json!({"id":"aaaaaaaaaaaaaaaaaaaaaaaaaa","name":"notes","type":"O"})),
            Response::json(json!({"id":"hhhhhhhhhhhhhhhhhhhhhhhhhh"})),
        ])
        .await;
        let tool = tool(&server.base, MattermostConfig::default(), ToolKind::Send);
        let outcome = tool.execute(json!({"channel":"aaaaaaaaaaaaaaaaaaaaaaaaaa","text":"**hello**","rootId":"bbbbbbbbbbbbbbbbbbbbbbbbbb"})).await.unwrap();
        assert!(!outcome.is_error, "{}", outcome.text);
        assert!(outcome.text.contains("hhhhhhhhhhhhhhhhhhhhhhhhhh"));
        let resolve = server.next().await;
        assert_eq!(resolve.path, "/api/v4/channels/aaaaaaaaaaaaaaaaaaaaaaaaaa");
        let post = server.next().await;
        assert_eq!(post.method, "POST");
        assert_eq!(post.path, "/api/v4/posts");
        assert_eq!(
            serde_json::from_slice::<Value>(&post.body).unwrap(),
            json!({"channel_id":"aaaaaaaaaaaaaaaaaaaaaaaaaa","message":"**hello**","root_id":"bbbbbbbbbbbbbbbbbbbbbbbbbb","file_ids":[]})
        );
        server.finish().await;
    }

    #[tokio::test]
    async fn resolved_channel_authorization_blocks_channels_and_raw_dm_bypass() {
        for case in ["channel", "disabled_dm", "restricted_dm"] {
            let mut cfg = MattermostConfig::default();
            let mut responses = vec![Response::json(
                json!({"id":"aaaaaaaaaaaaaaaaaaaaaaaaaa","name":"bbbbbbbbbbbbbbbbbbbbbbbbbb__cccccccccccccccccccccccccc","type":if case == "channel" {"O"} else {"D"}}),
            )];
            if case == "channel" {
                cfg.channels = Some(std::collections::HashMap::new());
            }
            if case == "disabled_dm" {
                cfg.dm.enabled = false;
            }
            if case == "restricted_dm" {
                cfg.dm.users = vec!["allowed".into()];
                responses.extend([
                    Response::json(json!({"id":"bbbbbbbbbbbbbbbbbbbbbbbbbb","username":"helper"})),
                    Response::json(
                        json!([{"id":"cccccccccccccccccccccccccc","username":"stranger"}]),
                    ),
                ]);
            }
            let server = Server::start(responses).await;
            let tool = tool(&server.base, cfg, ToolKind::Send);
            let outcome = tool
                .execute(json!({"channel":"aaaaaaaaaaaaaaaaaaaaaaaaaa","text":"hello"}))
                .await
                .unwrap();
            assert!(outcome.is_error, "{case}");
            assert_eq!(outcome.error.unwrap().code, "not_allowed", "{case}");
            server.finish().await;
        }
    }

    #[tokio::test]
    async fn channel_names_resolve_case_insensitively_and_ambiguity_requires_an_id() {
        for ambiguous in [false, true] {
            let mut channels = vec![
                json!({"id":"aaaaaaaaaaaaaaaaaaaaaaaaaa","name":"field-notes","display_name":"Field Notes","type":"P"}),
            ];
            if ambiguous {
                channels.push(
                    json!({"id":"bbbbbbbbbbbbbbbbbbbbbbbbbb","name":"field-notes","type":"O"}),
                );
            }
            let mut responses = vec![Response::json(json!(channels))];
            if !ambiguous {
                responses.push(Response::json(json!({"id":"hhhhhhhhhhhhhhhhhhhhhhhhhh"})));
            }
            let mut server = Server::start(responses).await;
            let tool = tool(&server.base, MattermostConfig::default(), ToolKind::Send);
            let outcome = tool
                .execute(json!({"channel":"#FIELD-NOTES","text":"hello"}))
                .await
                .unwrap();
            if ambiguous {
                assert_eq!(outcome.error.unwrap().code, "ambiguous_channel");
                assert!(outcome.text.contains("id"));
            } else {
                assert!(!outcome.is_error, "{}", outcome.text);
                assert!(outcome.text.contains("hhhhhhhhhhhhhhhhhhhhhhhhhh"));
            }
            assert_eq!(server.next().await.path, "/api/v4/users/me/channels");
            server.finish().await;
        }
    }

    #[tokio::test]
    async fn user_targets_resolve_ids_or_usernames_and_check_dm_allowlists_before_opening() {
        for by_id in [false, true] {
            for allowed in [false, true] {
                let user = json!({"id":"cccccccccccccccccccccccccc","username":"sage"});
                let mut responses = vec![Response::json(if by_id { json!([user]) } else { user })];
                if allowed {
                    responses.extend([
                Response::json(json!({"id":"bbbbbbbbbbbbbbbbbbbbbbbbbb","username":"helper"})),
                Response::json(json!({"id":"aaaaaaaaaaaaaaaaaaaaaaaaaa","name":"bbbbbbbbbbbbbbbbbbbbbbbbbb__cccccccccccccccccccccccccc","type":"D"})),
                Response::json(json!({"id":"bbbbbbbbbbbbbbbbbbbbbbbbbb","username":"helper"})),
                Response::json(json!({"id":"hhhhhhhhhhhhhhhhhhhhhhhhhh"})),
            ]);
                }
                let mut server = Server::start(responses).await;
                let mut cfg = MattermostConfig::default();
                cfg.dm.users = vec![if allowed { "@SAGE" } else { "someone-else" }.into()];
                let tool = tool(&server.base, cfg, ToolKind::Send);
                let outcome = tool.execute(json!({"user":if by_id {"cccccccccccccccccccccccccc"} else {"@SAGE"},"text":"hello"})).await.unwrap();
                if !allowed {
                    assert_eq!(outcome.error.unwrap().code, "not_allowed");
                } else {
                    assert!(!outcome.is_error, "{}", outcome.text);
                    assert!(outcome.text.contains("hhhhhhhhhhhhhhhhhhhhhhhhhh"));
                }
                let user_request = server.next().await;
                assert_eq!(
                    user_request.path,
                    if by_id {
                        "/api/v4/users/ids"
                    } else {
                        "/api/v4/users/username/sage"
                    }
                );
                if allowed {
                    server.next().await;
                    let direct = server.next().await;
                    assert_eq!(direct.path, "/api/v4/channels/direct");
                    assert_eq!(
                        serde_json::from_slice::<Value>(&direct.body).unwrap(),
                        json!(["bbbbbbbbbbbbbbbbbbbbbbbbbb", "cccccccccccccccccccccccccc"])
                    );
                }
                server.finish().await;
            }
        }
    }

    #[tokio::test]
    async fn send_splits_long_unicode_text_without_a_tool_length_cap() {
        let mut server = Server::start(vec![
            Response::json(json!({"id":"aaaaaaaaaaaaaaaaaaaaaaaaaa","type":"O"})),
            Response::json(json!({"id":"hhhhhhhhhhhhhhhhhhhhhhhhhh"})),
            Response::json(json!({"id":"iiiiiiiiiiiiiiiiiiiiiiiiii"})),
        ])
        .await;
        let tool = tool(&server.base, MattermostConfig::default(), ToolKind::Send);
        let text = "界".repeat(16_000);
        let outcome = tool
            .execute(json!({"channel":"aaaaaaaaaaaaaaaaaaaaaaaaaa","text":text}))
            .await
            .unwrap();
        assert!(!outcome.is_error, "{}", outcome.text);
        assert!(outcome.text.contains("iiiiiiiiiiiiiiiiiiiiiiiiii"));
        server.next().await;
        let one: Value = serde_json::from_slice(&server.next().await.body).unwrap();
        let two: Value = serde_json::from_slice(&server.next().await.body).unwrap();
        let first = one["message"].as_str().unwrap();
        let second = two["message"].as_str().unwrap();
        assert!(first.chars().count() <= 15_000 && second.chars().count() <= 15_000);
        assert_eq!(format!("{first}{second}"), text);
        server.finish().await;
    }

    #[tokio::test]
    async fn list_tools_filter_case_insensitively_and_return_public_identity_fields() {
        let mut server = Server::start(vec![Response::json(json!([
            {"id":"jjjjjjjjjjjjjjjjjjjjjjjjjj","username":"sage-helper","first_name":"Sage","last_name":"Helper","is_bot":true},
            {"id":"kkkkkkkkkkkkkkkkkkkkkkkkkk","username":"reader","nickname":"Reader"}
        ]))]).await;
        let users = tool(&server.base, MattermostConfig::default(), ToolKind::Users)
            .execute(json!({"query":"HELPER","limit":1}))
            .await
            .unwrap();
        assert_eq!(
            serde_json::from_str::<Value>(&users.text).unwrap(),
            json!([{"id":"jjjjjjjjjjjjjjjjjjjjjjjjjj","username":"sage-helper","name":"Sage Helper","bot":true}])
        );
        assert_eq!(server.next().await.path, "/api/v4/users/search");
        server.finish().await;
        let server = Server::start(vec![Response::json(json!([
            {"id":"llllllllllllllllllllllllll","name":"field-notes","display_name":"Field Notes","type":"P","team_id":"nnnnnnnnnnnnnnnnnnnnnnnnnn"},
            {"id":"mmmmmmmmmmmmmmmmmmmmmmmmmm","name":"other","display_name":"Other","type":"O","team_id":"nnnnnnnnnnnnnnnnnnnnnnnnnn"}
        ]))]).await;
        let channels = tool(
            &server.base,
            MattermostConfig::default(),
            ToolKind::Channels,
        )
        .execute(json!({"query":"NOTES","limit":1}))
        .await
        .unwrap();
        assert_eq!(
            serde_json::from_str::<Value>(&channels.text).unwrap(),
            json!([{"id":"llllllllllllllllllllllllll","name":"field-notes","displayName":"Field Notes","type":"P","teamId":"nnnnnnnnnnnnnnnnnnnnnnnnnn"}])
        );
        server.finish().await;
    }

    #[tokio::test]
    async fn scheduler_sink_uses_the_send_path_and_returns_tool_failures() {
        use dar_extension_sdk::deliver::{DeliverySink, Destination};
        let sink = tool(
            "http://127.0.0.1:9",
            MattermostConfig::default(),
            ToolKind::Send,
        );
        for destination in [
            Destination {
                channel: None,
                user: None,
            },
            Destination {
                channel: Some("room".into()),
                user: Some("sage".into()),
            },
        ] {
            assert!(sink
                .deliver(&destination, "scheduled result")
                .await
                .is_err());
        }
        for allowed in [false, true] {
            let mut responses = vec![Response::json(
                json!({"id":"aaaaaaaaaaaaaaaaaaaaaaaaaa","type":"O"}),
            )];
            if allowed {
                responses.push(Response::json(json!({"id":"oooooooooooooooooooooooooo"})));
            }
            let mut server = Server::start(responses).await;
            let mut cfg = MattermostConfig::default();
            if !allowed {
                cfg.channels = Some(std::collections::HashMap::new());
            }
            let sink = tool(&server.base, cfg, ToolKind::Send);
            let result = sink
                .deliver(
                    &Destination {
                        channel: Some("aaaaaaaaaaaaaaaaaaaaaaaaaa".into()),
                        user: None,
                    },
                    "scheduled result",
                )
                .await;
            assert_eq!(result.is_ok(), allowed);
            if allowed {
                server.next().await;
                let post: Value = serde_json::from_slice(&server.next().await.body).unwrap();
                assert_eq!(post["message"], "scheduled result");
            }
            server.finish().await;
        }
    }
}

mod artifact_tests {
    use crate::test_http::{Response, Server};
    use cap_chat::ArtifactReady;
    use dar_extension_sdk::artifacts::{
        ArtifactDeliveryTarget, ArtifactMetadataInput, ArtifactStore, DeliveryClaimResult,
        ExportRoot,
    };
    use mattermost::{api::MattermostClient, artifacts::deliver, config::SecretToken};
    use serde_json::{json, Value};
    fn artifact() -> (std::path::PathBuf, ArtifactStore, ArtifactReady) {
        static SEQUENCE: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        let root = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("target/test-artifacts")
            .join(format!(
                "{}-{}-{}",
                std::process::id(),
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_nanos(),
                SEQUENCE.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
            ));
        let export = root.join("export");
        std::fs::create_dir_all(&export).unwrap();
        std::fs::write(export.join("source.txt"), "artifact bytes").unwrap();
        let store = ArtifactStore::open(root.join("vault"), 1024).unwrap();
        let metadata = store
            .stage_from_export_root(
                &ExportRoot::open(export).unwrap(),
                "source.txt",
                ArtifactMetadataInput {
                    filename: "report.txt".into(),
                    media_type: Some("text/plain".into()),
                    caption: None,
                },
            )
            .unwrap();
        let ready = ArtifactReady {
            id: metadata.id,
            uri: format!("dar-artifact://{}", metadata.id),
            name: metadata.filename.clone(),
            mime_type: metadata.media_type.clone(),
            bytes: metadata.bytes,
            sha256: metadata.sha256_hex(),
            caption: None,
        };
        (root, store, ready)
    }
    fn target() -> ArtifactDeliveryTarget {
        ArtifactDeliveryTarget {surface_id:"mattermost".into(),origin_destination:json!({"channel":"aaaaaaaaaaaaaaaaaaaaaaaaaa","root_id":"bbbbbbbbbbbbbbbbbbbbbbbbbb"}).to_string()}
    }
    #[tokio::test]
    async fn artifact_delivery_uploads_verified_bytes_to_the_exact_thread_only_once() {
        let (root, store, artifact) = artifact();
        let mut server=Server::start(vec![Response::json(json!({"file_infos":[{"id":"cccccccccccccccccccccccccc","name":"report.txt","size":14}]})),Response::json(json!({"id":"dddddddddddddddddddddddddd"}))]).await;
        let client =
            MattermostClient::new(&server.base, SecretToken::new("invented-token".into())).unwrap();
        deliver(
            &store,
            &client,
            std::slice::from_ref(&artifact),
            "aaaaaaaaaaaaaaaaaaaaaaaaaa",
            Some("bbbbbbbbbbbbbbbbbbbbbbbbbb"),
        )
        .await
        .unwrap();
        deliver(
            &store,
            &client,
            std::slice::from_ref(&artifact),
            "aaaaaaaaaaaaaaaaaaaaaaaaaa",
            Some("bbbbbbbbbbbbbbbbbbbbbbbbbb"),
        )
        .await
        .unwrap();
        assert!(matches!(
            store.claim_delivery(artifact.id, target()).unwrap(),
            DeliveryClaimResult::Delivered(_)
        ));
        let upload = server.next().await;
        assert_eq!(upload.method, "POST");
        assert_eq!(
            upload.path,
            "/api/v4/files?channel_id=aaaaaaaaaaaaaaaaaaaaaaaaaa"
        );
        let multipart = String::from_utf8(upload.body).unwrap();
        assert!(multipart.contains("filename=\"report.txt\""));
        assert!(multipart.contains("artifact bytes"));
        let post = server.next().await;
        assert_eq!(post.path, "/api/v4/posts");
        assert_eq!(
            serde_json::from_slice::<Value>(&post.body).unwrap(),
            json!({"channel_id":"aaaaaaaaaaaaaaaaaaaaaaaaaa","message":"","root_id":"bbbbbbbbbbbbbbbbbbbbbbbbbb","file_ids":["cccccccccccccccccccccccccc"]})
        );
        server.finish().await;
        std::fs::remove_dir_all(root).unwrap();
    }
    #[tokio::test]
    async fn failed_upload_releases_the_claim_and_posts_a_visible_error() {
        let (root, store, artifact) = artifact();
        let mut server = Server::start(vec![
            Response::status(403),
            Response::json(json!({"id":"eeeeeeeeeeeeeeeeeeeeeeeeee"})),
        ])
        .await;
        let client =
            MattermostClient::new(&server.base, SecretToken::new("invented-token".into())).unwrap();
        deliver(
            &store,
            &client,
            std::slice::from_ref(&artifact),
            "aaaaaaaaaaaaaaaaaaaaaaaaaa",
            Some("bbbbbbbbbbbbbbbbbbbbbbbbbb"),
        )
        .await
        .unwrap();
        let claim = match store.claim_delivery(artifact.id, target()).unwrap() {
            DeliveryClaimResult::Claimed(claim) => claim,
            state => panic!("failed upload must be retryable: {state:?}"),
        };
        store.release_delivery(claim).unwrap();
        server.next().await;
        let post = tokio::time::timeout(std::time::Duration::from_secs(1), server.next())
            .await
            .expect("upload failure must produce a visible post");
        let body: Value = serde_json::from_slice(&post.body).unwrap();
        assert_eq!(body["message"], "Generated file could not be uploaded.");
        assert_eq!(body["root_id"], "bbbbbbbbbbbbbbbbbbbbbbbbbb");
        server.finish().await;
        std::fs::remove_dir_all(root).unwrap();
    }
    #[tokio::test]
    async fn upload_failure_returns_an_error_when_its_visible_notice_cannot_be_posted() {
        let (root, store, artifact) = artifact();
        let server = Server::start(vec![Response::status(403), Response::status(403)]).await;
        let client =
            MattermostClient::new(&server.base, SecretToken::new("invented-token".into())).unwrap();
        let result = deliver(
            &store,
            &client,
            std::slice::from_ref(&artifact),
            "aaaaaaaaaaaaaaaaaaaaaaaaaa",
            Some("bbbbbbbbbbbbbbbbbbbbbbbbbb"),
        )
        .await;
        assert!(
            result.is_err(),
            "runtime needs the failure to apply its warning fallback"
        );
        let claim = match store.claim_delivery(artifact.id, target()).unwrap() {
            DeliveryClaimResult::Claimed(claim) => claim,
            state => panic!("failed upload must release the claim: {state:?}"),
        };
        store.release_delivery(claim).unwrap();
        server.finish().await;
        std::fs::remove_dir_all(root).unwrap();
    }
    #[tokio::test]
    async fn claim_io_failures_are_visible_and_failed_notices_reach_the_runtime() {
        use std::os::unix::fs::PermissionsExt;
        for notice_sent in [true, false] {
            let (root, store, artifact) = artifact();
            let mut pending = vec![root.join("vault")];
            let mut permissions = Vec::new();
            while let Some(path) = pending.pop() {
                let metadata = std::fs::metadata(&path).unwrap();
                if metadata.is_dir() {
                    pending.extend(
                        std::fs::read_dir(&path)
                            .unwrap()
                            .map(|entry| entry.unwrap().path()),
                    );
                }
                permissions.push((path.clone(), metadata.permissions()));
                std::fs::set_permissions(
                    path,
                    std::fs::Permissions::from_mode(if metadata.is_dir() { 0o555 } else { 0o444 }),
                )
                .unwrap();
            }
            assert!(
                artifact.validate(&store).is_some(),
                "read-only storage still permits validation"
            );
            assert!(
                store.claim_delivery(artifact.id, target()).is_err(),
                "the public store must fail to write a delivery claim"
            );
            let mut server = Server::start(vec![if notice_sent {
                Response::json(json!({"id":"eeeeeeeeeeeeeeeeeeeeeeeeee"}))
            } else {
                Response::status(403)
            }])
            .await;
            let client =
                MattermostClient::new(&server.base, SecretToken::new("invented-token".into()))
                    .unwrap();
            let result = deliver(
                &store,
                &client,
                std::slice::from_ref(&artifact),
                "aaaaaaaaaaaaaaaaaaaaaaaaaa",
                Some("bbbbbbbbbbbbbbbbbbbbbbbbbb"),
            )
            .await;
            for (path, mode) in permissions {
                std::fs::set_permissions(path, mode).unwrap();
            }
            let post = tokio::time::timeout(std::time::Duration::from_secs(1), server.next())
                .await
                .expect("claim I/O failures must produce a visible post");
            assert_eq!(post.path, "/api/v4/posts");
            let body: Value = serde_json::from_slice(&post.body).unwrap();
            assert_eq!(body["message"], "Generated file could not be uploaded.");
            assert_eq!(body["root_id"], "bbbbbbbbbbbbbbbbbbbbbbbbbb");
            assert_eq!(
                result.is_ok(),
                notice_sent,
                "failed notices must reach runtime fallback"
            );
            server.finish().await;
            std::fs::remove_dir_all(root).unwrap();
        }
    }
}
