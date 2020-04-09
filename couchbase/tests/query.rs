mod util;

#[test]
fn run_query_tests() {
    util::run(|_cfg| {
        foo();
        bar();
    });
}

fn foo() {

}

fn bar() {

}