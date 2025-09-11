#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub enum NodeEdition {
    Community,
    Enterprise,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct NodeVersion {
    pub major: u8,
    pub minor: u8,
    pub patch: u8,
    pub build: u8,
    pub edition: Option<NodeEdition>,
    pub modifier: Option<String>,
}

impl NodeVersion {
    pub fn equal(&self, other: &NodeVersion) -> bool {
        self == other
    }

    pub fn higher(&self, other: &NodeVersion) -> bool {
        self > other
    }

    pub fn lower(&self, other: &NodeVersion) -> bool {
        self < other
    }
}

impl From<String> for NodeVersion {
    fn from(version: String) -> Self {
        let parts: Vec<&str> = version.split('.').collect();
        if parts.is_empty() {
            panic!("must provide at least a major version");
        }

        let major = parts[0].parse().unwrap();
        let minor = if parts.len() > 1 {
            parts[1].parse().unwrap()
        } else {
            0
        };
        let patch_build: Vec<&str> = if parts.len() > 2 {
            parts[2].split('-').collect()
        } else {
            vec!["0"]
        };
        let patch = patch_build[0].parse().unwrap();
        let build = if patch_build.len() > 1 {
            patch_build[1].parse().unwrap_or(0)
        } else {
            0
        };

        let edition_modifier: Vec<&str> = if patch_build.len() > 1 {
            patch_build[1].split('-').collect()
        } else {
            vec![]
        };
        let (edition, modifier) = if !edition_modifier.is_empty() {
            edition_modifier_from_str(edition_modifier[0]).unwrap()
        } else {
            (None, None)
        };

        NodeVersion {
            major,
            minor,
            patch,
            build,
            edition,
            modifier,
        }
    }
}

fn edition_modifier_from_str(
    edition_modifier: &str,
) -> Result<(Option<NodeEdition>, Option<String>), String> {
    let parts: Vec<&str> = edition_modifier.split('-').collect();
    let edition_str = parts[0].to_lowercase();
    let edition = match edition_str.as_str() {
        "enterprise" => Some(NodeEdition::Enterprise),
        "community" => Some(NodeEdition::Community),
        _ => None,
    };
    let modifier = if parts.len() > 1 {
        Some(parts[1].to_lowercase())
    } else {
        None
    };
    Ok((edition, modifier))
}
