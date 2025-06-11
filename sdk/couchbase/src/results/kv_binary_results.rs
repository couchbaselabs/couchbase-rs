use crate::mutation_state::MutationToken;

#[derive(Debug, Clone, Ord, PartialOrd, Eq, PartialEq, Hash)]
pub struct CounterResult {
    pub(crate) cas: u64,
    pub(crate) mutation_token: Option<MutationToken>,
    pub(crate) content: u64,
}

impl CounterResult {
    pub fn cas(&self) -> u64 {
        self.cas
    }

    pub fn mutation_token(&self) -> &Option<MutationToken> {
        &self.mutation_token
    }

    pub fn content(&self) -> u64 {
        self.content
    }
}
