// make a simple database file. The file will be a binary file with a series of ipc files.
// The start of each table with begin with a table name
// The ipc data will follow. This is the literal data which would normally be found in an individual file.

use arrow::array::{Int32Array, StringArray};
use arrow::datatypes::{Field, Schema};
use arrow::ipc::writer::FileWriter;
use arrow::record_batch::RecordBatch;
use std::fs;
use std::fs::File;
use std::fs::OpenOptions;
use std::io::prelude::*;
use std::io::{BufRead, BufReader};
use std::io::{BufWriter, Write};
use std::sync::Arc;
std::fs::read

fn main() {
    // table1
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

    let file = File::create("main.ylr").expect("Unable to create file");
    // write the table name in the file
    fs::write("main.ylr", "table_name\n").expect("Unable to write file");
    //let mut buffer = BufWriter::new(file);
    let mut file_open = OpenOptions::new()
        .write(true)
        .append(true)
        .open("main.ylr")
        .unwrap();
    let mut writer = FileWriter::try_new(file_open, &schema).unwrap();
    writer.write(&batch).unwrap();
    writer.finish().unwrap();

    // table 2
    let schema = Schema::new(vec![
        Field::new("col1", arrow::datatypes::DataType::Int32, false),
        Field::new("col2", arrow::datatypes::DataType::Utf8, false),
    ]);
    let batch = RecordBatch::try_new(
        Arc::new(schema.clone()),
        vec![
            Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5])),
            Arc::new(StringArray::from(vec!["yes", "can", "hear", "you", "!"])),
        ],
    )
    .unwrap();
    let mut file_open = OpenOptions::new()
        .write(true)
        .append(true)
        .open("main.ylr")
        .unwrap();
    file_open.write_all(b"\ntable_name2\n");
    let mut writer = FileWriter::try_new(file_open, &schema).unwrap();
    writer.write(&batch).unwrap();
    writer.finish().unwrap();

    // search for the table name in the file
    // read the file into a arrow::record_batch::RecordBatch
    // let mut contents = fs::read_to_string("main.ylr");

    // let mut start_reading = false;
    // let mut skip_line = true;
    // for line in contents.expect("REASON").lines() {
    //     if start_reading {
    //         println!("{}", line);
    //     } else if line.contains("table_name") {
    //         start_reading = true;
    //     }
    // }

    // read the file into an arrow::record_batch::RecordBatch
    // let file = File::open("main.ylr").unwrap();
    // let mut reader = arrow::ipc::reader::FileReader::try_new(file, None).unwrap();
    // let batch_read = reader.next().unwrap().unwrap();
    // println!("{:?}", batch_read);

    // read the ipc data that starts after table_name2 into an arrow::record_batch::RecordBatch
    
}
