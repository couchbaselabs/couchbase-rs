use crate::error::CoreError;
use crate::result::CoreResult;

#[derive(Debug, Clone, Eq, PartialEq)]
pub(crate) struct VbucketMap {
    entries: Vec<Vec<i16>>,
    num_replicas: usize,
}

impl VbucketMap {
    pub fn new(entries: Vec<Vec<i16>>, num_replicas: usize) -> CoreResult<Self> {
        if entries.is_empty() {
            return Err(CoreError::Placeholder(
                "vbucket map must have at least a single entry".to_string(),
            ));
        }

        Ok(Self {
            entries,
            num_replicas,
        })
    }

    pub fn is_valid(&self) -> bool {
        if let Some(entry) = self.entries.first() {
            return !entry.is_empty();
        }

        false
    }

    pub fn num_vbuckets(&self) -> usize {
        self.entries.len()
    }

    pub fn num_replicas(&self) -> usize {
        self.num_replicas
    }

    pub fn vbucket_by_key(&self, key: Vec<u8>) -> u16 {
        let checksum = crc32fast::hash(key.as_slice());
        let mid_bits = (checksum >> 16) as u16 & 0x7fff;
        mid_bits % (self.entries.len() as u16)
    }

    pub fn node_by_vbucket(&self, vb_id: u16, vb_server_idx: u32) -> CoreResult<i16> {
        let num_servers = (self.num_replicas as u32) + 1;
        if vb_server_idx > num_servers {
            return Err(CoreError::Placeholder("invalid replica".to_string()));
        }

        if let Some(idx) = self.entries.get(vb_id as usize) {
            if let Some(id) = idx.get(vb_server_idx as usize) {
                Ok(*id)
            } else {
                Ok(-1)
            }
        } else {
            Err(CoreError::Placeholder("invalid vbucket".to_string()))
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::vbucketmap::VbucketMap;

    #[test]
    fn vbucketmap_with_1024_vbs() {
        let vb_map = VbucketMap::new(vec![vec![]; 1024], 1).unwrap();

        assert_eq!(0x0202u16, vb_map.vbucket_by_key(vec![0]));
        assert_eq!(
            0x00aau16,
            vb_map.vbucket_by_key(vec![0, 1, 2, 3, 4, 5, 6, 7])
        );
        assert_eq!(0x0210u16, vb_map.vbucket_by_key(b"hello".to_vec()));
        assert_eq!(
            0x03d4u16,
            vb_map.vbucket_by_key(
                b"hello world, I am a super long key lets see if it works".to_vec()
            )
        );
    }

    #[test]
    fn vbucketmap_with_64_vbs() {
        let vb_map = VbucketMap::new(vec![vec![]; 64], 1).unwrap();

        assert_eq!(0x0002u16, vb_map.vbucket_by_key(vec![0]));
        assert_eq!(
            0x002au16,
            vb_map.vbucket_by_key(vec![0, 1, 2, 3, 4, 5, 6, 7])
        );
        assert_eq!(0x0010u16, vb_map.vbucket_by_key(b"hello".to_vec()));
        assert_eq!(
            0x0014u16,
            vb_map.vbucket_by_key(
                b"hello world, I am a super long key lets see if it works".to_vec()
            )
        );
    }

    #[test]
    fn vbucketmap_with_48_vbs() {
        let vb_map = VbucketMap::new(vec![vec![]; 48], 1).unwrap();

        assert_eq!(0x0012u16, vb_map.vbucket_by_key(vec![0]));
        assert_eq!(
            0x000au16,
            vb_map.vbucket_by_key(vec![0, 1, 2, 3, 4, 5, 6, 7])
        );
        assert_eq!(0x0010u16, vb_map.vbucket_by_key(b"hello".to_vec()));
        assert_eq!(
            0x0004u16,
            vb_map.vbucket_by_key(
                b"hello world, I am a super long key lets see if it works".to_vec()
            )
        );
    }

    #[test]
    fn vbucketmap_with_13_vbs() {
        let vb_map = VbucketMap::new(vec![vec![]; 13], 1).unwrap();

        assert_eq!(0x000cu16, vb_map.vbucket_by_key(vec![0]));
        assert_eq!(
            0x0008u16,
            vb_map.vbucket_by_key(vec![0, 1, 2, 3, 4, 5, 6, 7])
        );
        assert_eq!(0x0008u16, vb_map.vbucket_by_key(b"hello".to_vec()));
        assert_eq!(
            0x0003u16,
            vb_map.vbucket_by_key(
                b"hello world, I am a super long key lets see if it works".to_vec()
            )
        );
    }
}
