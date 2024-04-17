#[derive(Debug, Copy, Clone)]
pub struct NodeVersion {
    pub major: u32,
    pub minor: u32,
    pub patch: u32,
}

impl NodeVersion {
    pub fn higher(&self, ov: &NodeVersion) -> bool {
        if self.major > ov.major {
            return true;
        }
        if self.minor > ov.minor {
            return true;
        }
        if self.patch > ov.patch {
            return true;
        }

        false
    }
    pub fn lower(&self, ov: &NodeVersion) -> bool {
        !self.higher(ov) && !self.equal(ov)
    }
    pub fn equal(&self, ov: &NodeVersion) -> bool {
        if self.major == ov.major && self.minor == ov.minor && self.patch == ov.patch {
            return true;
        }

        false
    }
}

impl From<String> for NodeVersion {
    fn from(value: String) -> Self {
        let split_value: Vec<String> = value.split(".").map(String::from).collect();
        if split_value.len() != 3 {
            panic!("Server version must be of form 7.0.0");
        }

        let major: u32 = split_value.get(0).unwrap().parse().unwrap();
        let minor: u32 = split_value.get(1).unwrap().parse().unwrap();
        let patch: u32 = split_value.get(2).unwrap().parse().unwrap();

        NodeVersion {
            major,
            minor,
            patch,
        }
    }
}
