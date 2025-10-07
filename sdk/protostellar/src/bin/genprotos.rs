/*
 *
 *  * Copyright (c) 2025 Couchbase, Inc.
 *  *
 *  * Licensed under the Apache License, Version 2.0 (the "License");
 *  * you may not use this file except in compliance with the License.
 *  * You may obtain a copy of the License at
 *  *
 *  *    http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

use std::path::PathBuf;

use glob::glob;

fn main() {
    let proto_files = get_protos();
    let proto_include_paths = ["contrib/protostellar", "contrib/googleapis"];

    tonic_build::configure()
        .build_server(false)
        .out_dir("genproto")
        .protoc_arg("--experimental_allow_proto3_optional") // Need this for the linux build
        .compile_protos(&proto_files, &proto_include_paths)
        .unwrap_or_else(|e| panic!("Failed to compile protos {e:?}"));

    for proto in &proto_files {
        println!(
            "cargo:rerun-if-changed={}",
            proto.to_str().expect("Failed converting PathBuf to str")
        );
    }
}

fn get_protos() -> Vec<PathBuf> {
    let mut protos: Vec<PathBuf> = vec![];
    for entry in glob("contrib/protostellar/**/*.proto").expect("Could not glob protostellar files")
    {
        match entry {
            Ok(path) => protos.push(path.clone()),
            Err(e) => panic!("{e:?}"),
        }
    }
    protos
}
