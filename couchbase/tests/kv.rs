mod util;

#[test]
fn run_kv_tests() {
    util::run(|_cfg| {
        foo();
        bar();
    });
}

fn foo() {}

fn bar() {}
