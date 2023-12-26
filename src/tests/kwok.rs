//! Test helper to create a temporary kwok cluster.
use std::{convert::TryFrom, env, fs, path::PathBuf, process::Command, sync::Mutex};

use kube::{config::Kubeconfig, Client, Config};

// Mutex to secure port selection for the cluster
static CLUSTER_MUTEX : Mutex<i32> = Mutex::new(1);

/// Struct to manage a temporary kwok cluster.
#[derive(Debug, Default)]
pub struct TestEnv {
    // The name of the temporary cluster.
    name: String,
    // Kubeconfig of the temporary cluster.
    kubeconfig: Kubeconfig,
}

impl TestEnv {
    /// Builder for configuring the test environemnt.
    pub fn builder() -> TestEnvBuilder {
        Default::default()
    }

    /// Create the default minimal test environment.
    pub fn new() -> Self {
        Self::builder().build()
    }

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

        fs::remove_file(temp_kubeconfig(self.name.clone()))
            .expect("remove temporary kubeconfig location");
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
}

impl Drop for TestEnv {
    fn drop(&mut self) {
        self.delete();
    }
}

/// Builder for [`TestEnv`] to customize the environment.
#[derive(Debug)]
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
    pub fn wait(&mut self, v: String) -> &mut Self {
        self.wait = v;
        self
    }

    /// Set kubeconfig path flag.
    pub fn kubeconfig(&mut self, v: String) -> &mut Self {
        self.kubeconfig = v;
        self
    }

    /// Set the insecure_skip_tls_verify kubeconfig flag.
    pub fn insecure_skip_tls_verify(&mut self, v: bool) -> &mut Self {
        self.insecure_skip_tls_verify = v;
        self
    }

    /// Create the test environment.
    pub fn build(&self) -> TestEnv {
        let _guard = CLUSTER_MUTEX.lock().unwrap();
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
            "" => temp_kubeconfig(cluster),
            kubeconfig => kubeconfig.into(),
        };

        args.push("--kubeconfig");
        args.push(kubeconfig.to_str().unwrap());

        let status = Command::new("kwokctl")
            .args(&args)
            .status()
            .expect("kwok create cluster");
        assert!(status.success(), "failed to create kwok cluster");

        let mut args = vec!["get", "kubeconfig", "--name", &name];
        if self.insecure_skip_tls_verify {
            args.push("--insecure-skip-tls-verify")
        }

        // Output the cluster's kubeconfig to stdout and store it.
        let stdout = Command::new("kwokctl")
            .args(&args)
            .output()
            .expect("kwok kubeconfig get failed")
            .stdout;
        let stdout = std::str::from_utf8(&stdout).expect("valid string");

        TestEnv {
            name,
            kubeconfig: serde_yaml::from_str(stdout).expect("valid kubeconfig"),
        }
    }
}

fn temp_kubeconfig(cluster: String) -> PathBuf {
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
            .build();
        let pod_api: Api<DynamicObject> =
            Api::default_namespaced_with(test_env.client().await, &ApiResource::erase::<Pod>(&()));

        pod_api.list(&ListParams::default()).await.unwrap();
    }
}
