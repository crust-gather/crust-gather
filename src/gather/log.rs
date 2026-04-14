use std::fmt::Display;

use anyhow::anyhow;
use serde::Deserialize;

#[derive(Clone, Default, Deserialize, Debug)]
pub struct UserLog {
    pub name: String,
    pub command: String,
}

impl TryFrom<&str> for UserLog {
    type Error = anyhow::Error;

    fn try_from(s: &str) -> Result<Self, Self::Error> {
        let (name, command) = s.split_once(':').unwrap_or_default();
        if name.is_empty() && command.is_empty() {
            Err(anyhow!("Custom log should contain : delimiter"))?;
        }

        Ok(Self {
            name: name.into(),
            command: command.into(),
        })
    }
}

impl Display for UserLog {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "<File: {}, Command: {}>", self.name, self.command)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn try_from_parses_name_and_command() {
        let log = UserLog::try_from("kubelet.log:journalctl -u kubelet").unwrap();

        assert_eq!(log.name, "kubelet.log");
        assert_eq!(log.command, "journalctl -u kubelet");
    }

    #[test]
    fn try_from_allows_empty_name_or_command_when_delimiter_present() {
        let missing_name = UserLog::try_from(":echo test").unwrap();
        assert_eq!(missing_name.name, "");
        assert_eq!(missing_name.command, "echo test");

        let missing_command = UserLog::try_from("kubelet.log:").unwrap();
        assert_eq!(missing_command.name, "kubelet.log");
        assert_eq!(missing_command.command, "");
    }

    #[test]
    fn try_from_rejects_missing_delimiter() {
        let error = UserLog::try_from("kubelet.log").unwrap_err();

        assert_eq!(error.to_string(), "Custom log should contain : delimiter");
    }

    #[test]
    fn display_formats_user_log() {
        let log = UserLog {
            name: "kubelet.log".into(),
            command: "journalctl -u kubelet".into(),
        };

        assert_eq!(
            log.to_string(),
            "<File: kubelet.log, Command: journalctl -u kubelet>"
        );
    }
}
