//! Test helper to create a temporary kwok cluster.
use std::{
    convert::TryFrom,
    env, fs,
    path::PathBuf,
    process::{Command, Stdio},
    time::Duration,
};

use anyhow::bail;
use kube::{config::Kubeconfig, Client, Config};
use tokio_retry::{
    strategy::{jitter, FixedInterval},
    Retry,
};

/// Struct to manage a temporary kwok cluster.
#[derive(Default)]
pub struct TestEnv {
    // The name of the temporary cluster.
    name: String,
    // Kubeconfig of the temporary cluster.
    kubeconfig: Kubeconfig,
}

/// TestEnv manages a temporary Kwok test cluster.
///
/// It provides methods to create, delete and interact with the minimal test cluster.
/// The cluster is automatically deleted when this struct is dropped.
impl TestEnv {
    /// Builder for configuring the test environemnt.
    pub fn builder() -> TestEnvBuilder {
        Default::default()
    }

    /// Create the default minimal test environment.
    pub async fn new() -> Self {
        Self::builder().build().await
    }

    /// Deletes the temporary Kwok cluster and removes the kubeconfig file.
    ///
    /// Runs `kwokctl delete` to delete the cluster and removes the generated
    /// kubeconfig file for this cluster.
    fn delete(&mut self) {
        let status = Command::new("kwokctl")
            .args(&["delete", "cluster", "--name", &self.name])
            .status()
            .expect("kwok cluster delete failed");
        assert!(
            status.success(),
            "kwok cluster delete failed. cluster {} may still exist",
            self.name
        );

        fs::remove_file(self.kubeconfig_path()).expect("remove temporary kubeconfig location");
    }

    /// Create a new `Client` configured for the temporary server.
    pub async fn client(&self) -> Client {
        assert_eq!(
            self.kubeconfig.clusters.len(),
            1,
            "kubeconfig only contains the temporary cluster"
        );

        let config = Config::from_custom_kubeconfig(self.kubeconfig.clone(), &Default::default())
            .await
            .expect("valid kubeconfig");
        Client::try_from(config).expect("client")
    }

    /// Returns the Kubeconfig for this TestEnv.
    pub fn kubeconfig(&self) -> Kubeconfig {
        self.kubeconfig.clone()
    }

    /// Returns the path to the Kubeconfig file for this TestEnv.
    pub fn kubeconfig_path(&self) -> PathBuf {
        temp_kubeconfig(&self.name)
    }
}

impl Drop for TestEnv {
    fn drop(&mut self) {
        self.delete();
    }
}

/// Builder for [`TestEnv`] to customize the environment.
pub struct TestEnvBuilder {
    verbose: u8,
    wait: String,
    kubeconfig: String,
    insecure_skip_tls_verify: bool,
}

impl Default for TestEnvBuilder {
    fn default() -> Self {
        Self {
            verbose: 0,
            wait: "10s".into(),
            kubeconfig: "".into(),
            insecure_skip_tls_verify: false,
        }
    }
}

impl TestEnvBuilder {
    /// Set `verbose` flag.
    pub fn verbose(&mut self, v: u8) -> &mut Self {
        self.verbose = v;
        self
    }

    /// Set duration for `wait` flag.
    pub fn wait(&mut self, v: &str) -> &mut Self {
        self.wait = v.into();
        self
    }

    /// Set kubeconfig path flag.
    pub fn kubeconfig(&mut self, v: &str) -> &mut Self {
        self.kubeconfig = v.into();
        self
    }

    /// Set the insecure_skip_tls_verify kubeconfig flag.
    pub fn insecure_skip_tls_verify(&mut self, v: bool) -> &mut Self {
        self.insecure_skip_tls_verify = v;
        self
    }

    /// Create the test environment.
    pub async fn build(&self) -> TestEnv {
        let cluster = xid::new().to_string();
        let name = cluster.clone();
        let mut args = vec![
            "create",
            "cluster",
            "--name",
            name.as_str(),
            "--wait",
            self.wait.as_str(),
        ];

        let v = self.verbose.to_string();
        if self.verbose > 0 {
            args.push("--v");
            args.push(v.as_str());
        }

        let kubeconfig = match self.kubeconfig.as_str() {
            "" => temp_kubeconfig(cluster.as_str()),
            kubeconfig => kubeconfig.into(),
        };

        args.push("--kubeconfig");
        args.push(kubeconfig.to_str().unwrap());

        async fn test() -> anyhow::Result<()> {
            Ok(())
        }

        Retry::spawn(
            FixedInterval::new(jitter(Duration::from_secs(10))),
            || async {
                match Command::new("kwokctl")
                    .args(&args)
                    .stderr(Stdio::null())
                    .status()
                {
                    Ok(status) => match status.success() {
                        true => Ok(()),
                        false => bail!("Cluster create failed"),
                    },
                    Err(e) => bail!(e),
                }
            },
        )
        .await
        .expect("kwok create cluster");

        let mut args = vec!["get", "kubeconfig", "--name", &name];
        if self.insecure_skip_tls_verify {
            args.push("--insecure-skip-tls-verify")
        }

        // Output the cluster's kubeconfig to stdout and store it.
        let stdout = Command::new("kwokctl")
            .args(&args)
            .output()
            .expect("kwokctl get kubeconfig failed")
            .stdout;
        let stdout = std::str::from_utf8(&stdout).expect("valid string");

        TestEnv {
            name,
            kubeconfig: serde_yaml::from_str(stdout).expect("valid kubeconfig"),
        }
    }
}

fn temp_kubeconfig(cluster: &str) -> PathBuf {
    let mut dir = env::temp_dir();
    dir.push(cluster);
    dir
}

#[cfg(test)]
mod test {
    use k8s_openapi::api::core::v1::Pod;
    use kube::Api;
    use kube_core::{params::ListParams, ApiResource, DynamicObject};

    use crate::tests::kwok;

    #[tokio::test]
    async fn sanity() {
        let test_env = kwok::TestEnvBuilder::default()
            .insecure_skip_tls_verify(true)
            .build()
            .await;
        let pod_api: Api<DynamicObject> =
            Api::default_namespaced_with(test_env.client().await, &ApiResource::erase::<Pod>(&()));

        pod_api.list(&ListParams::default()).await.unwrap();
    }
}
