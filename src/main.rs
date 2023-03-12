use std::fs::File;
use std::io::prelude::*;
use std::str::from_utf8;
use tar::{Archive, Builder, Entry};

fn main() {
    //std::fs::write("file1.txt", b"hello").unwrap();
    //std::fs::write("file2.txt", b"world").unwrap();
    // write tar with file1 and file2
    let file = File::create("foo.tar").unwrap();
    let mut a = Builder::new(file);
    a.append_path("file1.txt").expect("file1 not found");
    a.append_path("file2.txt").expect("file2 not found");
    // read file2 without unpack
    let file = File::open("foo.tar").unwrap();
    let mut a = Archive::new(file);
    // file_contents
    let mut file_contents: Option<Entry<'_, std::fs::File>> = Option::None;
    for file in a.entries().unwrap() {
        let file = file.unwrap();
        let file_name = file.header().path().unwrap();
        println!("File name: {}", file_name.display());
        // get file contents if the file is file2
        match file_name.to_str().unwrap() {
            "file2.txt" => {
                file_contents = Some(file);
                break;
            }
            _ => continue,
        };
    }
    let mut buffer = Vec::new();
    file_contents.unwrap().read_to_end(&mut buffer).unwrap();
    println!("File contents: [{}]", from_utf8(&buffer).unwrap());
}
