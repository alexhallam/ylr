use std::fs::File;
use std::io::prelude::*;
use std::str::from_utf8;
use tar::{Archive, Builder, Entry};

use arrow2::io::ipc::read::deserialize_schema;
use arrow2::io::ipc::read::read_file_metadata;
use arrow2::io::ipc::read::FileMetadata;
use arrow2::io::ipc::read::StreamReader;
use std::io::{BufReader, Read, Seek};

use arrow::array::Array;
use arrow::array::{Int32Array, StringArray};
use arrow::datatypes::{Field, Schema};
use arrow::ipc::writer::FileWriter;
use arrow::record_batch::RecordBatch;
use arrow2::io::ipc::read::FileReader;
use std::sync::Arc;

fn write_files() {
    // create a schema for data
    let schema = Schema::new(vec![
        Field::new("col1", arrow::datatypes::DataType::Int32, false),
        Field::new("col2", arrow::datatypes::DataType::Utf8, false),
    ]);

    // create a record batch
    let batch = RecordBatch::try_new(
        Arc::new(schema.clone()),
        vec![
            Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5])),
            Arc::new(StringArray::from(vec!["can", "you", "hear", "me", "?"])),
        ],
    )
    .unwrap();

    let file = File::create("test.arrow").unwrap();
    let mut writer = FileWriter::try_new(file, &schema).unwrap();
    writer.write(&batch).unwrap();
    writer.finish().unwrap();

    let schema = Schema::new(vec![
        Field::new("col1", arrow::datatypes::DataType::Int32, false),
        Field::new("col2", arrow::datatypes::DataType::Utf8, false),
    ]);

    // create a record batch
    let batch = RecordBatch::try_new(
        Arc::new(schema.clone()),
        vec![
            Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5])),
            Arc::new(StringArray::from(vec!["I", "can", "hear", "you", "!"])),
        ],
    )
    .unwrap();

    let file = File::create("test2.arrow").unwrap();
    let mut writer = FileWriter::try_new(file, &schema).unwrap();
    writer.write(&batch).unwrap();
    writer.finish().unwrap();

    // read the file into an arrow::record_batch::RecordBatch
    let file = File::open("test.arrow").unwrap();
    let mut reader = arrow::ipc::reader::FileReader::try_new(file, None).unwrap();
    let batch_read = reader.next().unwrap().unwrap();
    println!("{:?}", batch_read);
}

fn main() {
    write_files();
    // write tar with file1 and file2
    let file_to_read = "df/test.arrow";
    let file = File::create("foo.tar").unwrap();
    let mut a = Builder::new(file);
    a.append_path("df/test.arrow").expect("file1 not found");
    a.append_path("df/test2.arrow").expect("file2 not found");

    // read file2 without unpacking
    let file = File::open("foo.tar").unwrap();
    let mut a = Archive::new(file);

    // file_contents
    let mut file_contents = None;
    for file in a.entries().unwrap() {
        let file = file.unwrap();
        let file_name = file.header().path().unwrap();
        println!("File name: {}", file_name.display());

        // get file contents if the file is file2
        match file_name.to_str().unwrap() {
            "df/test.arrow" => {
                file_contents = Some(file);
                break;
            }
            _ => continue,
        };
    }

    let archive_contents = a.into_inner();
    let entry_contents = file_contents.unwrap();
    println!("File contents: [{:?}]", entry_contents.header());
    let mut buf_reader = BufReader::new(file_contents.unwrap());
    let mut buffer = Vec::new();
    buf_reader.read_to_end(&mut buffer).unwrap();
    let mut buffer_slice = buffer.as_slice();

    //let metadata: FileMetadata = read_file_metadata(&mut buf_reader).unwrap();
    //println!("File contents: [{:?}]", metadata);
}
