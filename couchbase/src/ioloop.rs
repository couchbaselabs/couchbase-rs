use volatile::Volatile;

struct IoLoop {
    active: Volatile<bool>,
}

impl IoLoop {
    fn new() -> Self {
        IoLoop { active: Volatile::new(true) }
    }

    fn run(&self) {
        while self.active.read() {

        }
    }

    fn stop(&mut self) {
        self.active.write(false);
    }
}
