#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct MutationToken {
    pub(crate) vbid: u16,
    pub(crate) vbuuid: u64,
    pub(crate) seqno: u64,
}

impl MutationToken {
    pub fn new(vbid: u16, vbuuid: u64, seqno: u64) -> Self {
        Self {
            vbid,
            vbuuid,
            seqno,
        }
    }

    pub fn vbid(&self) -> u16 {
        self.vbid
    }

    pub fn vbuuid(&self) -> u64 {
        self.vbuuid
    }

    pub fn seqno(&self) -> u64 {
        self.seqno
    }

    pub fn set_vbid(&mut self, vbid: u16) {
        self.vbid = vbid;
    }

    pub fn set_vbuuid(&mut self, vbuuid: u64) {
        self.vbuuid = vbuuid;
    }

    pub fn set_seqno(&mut self, seqno: u64) {
        self.seqno = seqno;
    }
}
