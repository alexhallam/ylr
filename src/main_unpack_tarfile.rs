// write ipc to tar
use std::fs::File;
use std::io::{self, Write};
use std::sync::Arc;
use tempdir::TempDir;

use tar::Archive;

fn main() {
    // use tar to write ipc to tar
    // let file = File::create("main.ylr").unwrap();
    // let mut a = Builder::new(file);

    // a.append_path("test.arrow").unwrap();
    // a.append_path("test2.arrow").unwrap();
    let file = File::open("main.tar").unwrap();
    let mut ar = Archive::new(file);
    //ar.unpack("test.arrow").unwrap();

    for (i, file) in ar.entries().unwrap().enumerate() {
        let mut file = file.unwrap();
        if file.path().unwrap().display().to_string() == "test.arrow" {
            println!("File {}:{}", i, file.path().unwrap().display());
            file.unpack_in("df").unwrap();
        }
    }
}
