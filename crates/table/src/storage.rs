//! Object store construction from URLs and the put-if-absent self-test.

use std::collections::HashMap;
use std::sync::Arc;

use object_store::aws::{AmazonS3Builder, AmazonS3ConfigKey};
use object_store::azure::{AzureConfigKey, MicrosoftAzureBuilder};
use object_store::gcp::{GoogleCloudStorageBuilder, GoogleConfigKey};
use object_store::local::LocalFileSystem;
use object_store::memory::InMemory;
use object_store::path::Path;
use object_store::{ObjectStore, ObjectStoreExt, ObjectStoreScheme, PutMode, PutOptions};
use url::Url;

use crate::error::{Error, Result};

/// Opens the object store for a table URL such as `s3://bucket/prefix`,
/// `az://container/prefix`, `gs://bucket/prefix`, `file:///mnt/nfs/x` or `memory:///`.
///
/// Cloud credentials are taken from the usual environment variables; `options`
/// (e.g. `aws_endpoint`, `aws_region`) override them.
pub fn open_store(
    url: &str,
    options: &HashMap<String, String>,
) -> Result<(Arc<dyn ObjectStore>, Path)> {
    let url =
        Url::parse(url).map_err(|e| Error::Config(format!("invalid storage url {url}: {e}")))?;
    let (scheme, path) = ObjectStoreScheme::parse(&url)
        .map_err(|e| Error::Config(format!("unsupported storage url {url}: {e}")))?;
    let store: Arc<dyn ObjectStore> = match scheme {
        ObjectStoreScheme::Memory => Arc::new(InMemory::new()),
        ObjectStoreScheme::Local => {
            let dir = url
                .to_file_path()
                .map_err(|_| Error::Config(format!("invalid file url {url}")))?;
            std::fs::create_dir_all(&dir)?;
            return Ok((
                Arc::new(LocalFileSystem::new_with_prefix(dir)?),
                Path::default(),
            ));
        }
        ObjectStoreScheme::AmazonS3 => {
            let mut b = AmazonS3Builder::from_env().with_url(url.as_str());
            for (k, v) in options {
                let key: AmazonS3ConfigKey = k
                    .parse()
                    .map_err(|e| Error::Config(format!("storage option {k}: {e}")))?;
                b = b.with_config(key, v);
            }
            Arc::new(b.build()?)
        }
        ObjectStoreScheme::MicrosoftAzure => {
            let mut b = MicrosoftAzureBuilder::from_env().with_url(url.as_str());
            for (k, v) in options {
                let key: AzureConfigKey = k
                    .parse()
                    .map_err(|e| Error::Config(format!("storage option {k}: {e}")))?;
                b = b.with_config(key, v);
            }
            Arc::new(b.build()?)
        }
        ObjectStoreScheme::GoogleCloudStorage => {
            let mut b = GoogleCloudStorageBuilder::from_env().with_url(url.as_str());
            for (k, v) in options {
                let key: GoogleConfigKey = k
                    .parse()
                    .map_err(|e| Error::Config(format!("storage option {k}: {e}")))?;
                b = b.with_config(key, v);
            }
            Arc::new(b.build()?)
        }
        other => {
            return Err(Error::Config(format!(
                "storage scheme {other:?} does not support conditional writes"
            )));
        }
    };
    Ok((store, path))
}

/// Verifies that the store rejects a second put-if-absent of the same object.
/// Without that guarantee exactly-once commits are impossible.
pub async fn self_test(store: &dyn ObjectStore, root: &Path) -> Result<()> {
    let probe = root
        .clone()
        .join("_log")
        .clone()
        .join(format!("_probe-{}", uuid::Uuid::now_v7()));
    let create = || PutOptions {
        mode: PutMode::Create,
        ..Default::default()
    };
    store.put_opts(&probe, "1".into(), create()).await?;
    let second = store.put_opts(&probe, "2".into(), create()).await;
    let _ = store.delete(&probe).await;
    match second {
        Err(object_store::Error::AlreadyExists { .. }) => Ok(()),
        Ok(_) => Err(Error::Config(
            "storage accepted a second put-if-absent of the same object; \
             it does not support conditional writes (If-None-Match), refusing to start"
                .into(),
        )),
        Err(e) => Err(e.into()),
    }
}

pub(crate) fn join(root: &Path, rel: &str) -> Path {
    rel.split('/')
        .filter(|p| !p.is_empty())
        .fold(root.clone(), |p, part| p.join(part))
}
