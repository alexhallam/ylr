// write a ipc file

use arrow::array::Array;
use arrow::array::{Int32Array, StringArray};
use arrow::datatypes::{Field, Schema};
use arrow::ipc::writer::FileWriter;
use arrow::record_batch::RecordBatch;
use arrow2::io::ipc::read::{read_file_metadata, FileReader};
use std::fs::File;
use std::sync::Arc;

fn main() {
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
