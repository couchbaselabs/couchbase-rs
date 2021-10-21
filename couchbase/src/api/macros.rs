/// Macro to DRY up the repetitive timeout setter.
macro_rules! timeout {
    () => {
        pub fn timeout(mut self, timeout: Duration) -> Self {
            self.timeout = Some(timeout);
            self
        }
    };
}

macro_rules! expiry {
    () => {
        pub fn expiry(mut self, expiry: Duration) -> Self {
            self.expiry = Some(expiry);
            self
        }
    };
}

macro_rules! xattr {
    () => {
        pub fn xattr(mut self, xattr: bool) -> Self {
            self.xattr = xattr;
            self
        }
    };
}

macro_rules! preserve_expiry {
    () => {
        pub fn preserve_expiry(mut self, preserve: bool) -> Self {
            self.preserve_expiry = preserve;
            self
        }
    };
}

macro_rules! unwrap_or_default {
    ($opt:expr) => {
        $opt.unwrap_or_else(Default::default)
    };
}

macro_rules! durability {
    () => {
        pub fn durability(mut self, level: DurabilityLevel) -> Self {
            self.durability = Some(level);
            self
        }
    };
}
