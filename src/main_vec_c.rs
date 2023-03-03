use std::sync::Arc;

use arrow::array::BooleanArray;
use arrow::compute::is_null;
use arrow::datatypes::DataType::{Boolean, Date32, Float64, Int64, Null, Timestamp, Utf8};
use arrow::datatypes::TimeUnit::Second;
use arrow::datatypes::{BooleanType, Date32Type, Float64Type, Int64Type, Utf8Type};
use arrow::ipc::Bool;
use arrow::{array::*, datatypes::Int32Type};
use chrono::{NaiveDate, NaiveDateTime};
struct vec_ptype {
    name: String,
    size: u64,
    data_type: String,
}

struct vctr {
    // how can i have an arrow array of any type here?
    vec: ArrayRef,
    vec_ptype: vec_ptype,
}

struct tibble {
    data_frame: Vec<vctr>,
}

#[derive(Debug)]
enum ArrayProtype {
    Float64(Float64Array),
    Int64(Int64Array),
    String(StringArray),
    Boolean(BooleanArray),
    Date32(Date32Array),
    Timestamp(TimestampSecondArray),
    Null(NullArray),
}
impl From<Float64Array> for ArrayProtype {
    fn from(x: Float64Array) -> ArrayProtype {
        ArrayProtype::Float64(x)
    }
}
impl From<Int64Array> for ArrayProtype {
    fn from(x: Int64Array) -> ArrayProtype {
        ArrayProtype::Int64(x)
    }
}
impl From<StringArray> for ArrayProtype {
    fn from(x: StringArray) -> ArrayProtype {
        ArrayProtype::String(x)
    }
}
impl From<BooleanArray> for ArrayProtype {
    fn from(x: BooleanArray) -> ArrayProtype {
        ArrayProtype::Boolean(x)
    }
}
impl From<Date32Array> for ArrayProtype {
    fn from(x: Date32Array) -> ArrayProtype {
        ArrayProtype::Date32(x)
    }
}
impl From<TimestampSecondArray> for ArrayProtype {
    fn from(x: TimestampSecondArray) -> ArrayProtype {
        ArrayProtype::Timestamp(x)
    }
}
impl From<NullArray> for ArrayProtype {
    fn from(x: NullArray) -> ArrayProtype {
        ArrayProtype::Null(x)
    }
}

#[derive(Debug)]
enum ValueRaw {
    Null(arrow::datatypes::DataType),
    Int64(i64),
    Float64(f64),
    Utf8(String),
    Boolean(bool),
    Date(NaiveDate),         // chrono::NaiveDate
    DateTime(NaiveDateTime), // chrono::NaiveDateTime
}

impl From<i64> for ValueRaw {
    fn from(x: i64) -> ValueRaw {
        ValueRaw::Int64(x)
    }
}
impl From<f64> for ValueRaw {
    fn from(x: f64) -> ValueRaw {
        ValueRaw::Float64(x)
    }
}
impl From<String> for ValueRaw {
    fn from(x: String) -> ValueRaw {
        ValueRaw::Utf8(x)
    }
}
impl From<bool> for ValueRaw {
    fn from(x: bool) -> ValueRaw {
        ValueRaw::Boolean(x)
    }
}
impl From<&str> for ValueRaw {
    // check to see if a string is a date or datetime else return a string
    fn from(x: &str) -> ValueRaw {
        let parsed_date = NaiveDate::parse_from_str(x, "%Y-%m-%d");
        let parsed_date_time = NaiveDateTime::parse_from_str(x, "%Y-%m-%d %H:%M:%S");
        if parsed_date.is_ok() {
            // is date
            ValueRaw::Date(parsed_date.unwrap())
        } else if parsed_date_time.is_ok() {
            // is datetime
            ValueRaw::DateTime(parsed_date_time.unwrap())
        } else {
            // is string
            ValueRaw::Utf8(x.to_owned())
        }
    }
}

// impliment check for NA
impl From<arrow::datatypes::DataType> for ValueRaw {
    fn from(x: arrow::datatypes::DataType) -> ValueRaw {
        ValueRaw::Null(x)
    }
}

macro_rules! vec_c {
    ($($x:expr),+ $(,)?) => {
        {
            // need this intermediate "let" expression so that
            // the type gets inferred correctly without requiring
            // extra work for the user
            let v: Vec<ValueRaw> = vec![
                $($x.into()),+
            ];
            v;
            let mut data_types: Vec<arrow::datatypes::DataType> = Vec::new();
            for i in v.iter() {
                match i {
                    ValueRaw::Null(x) => data_types.push(x.clone()),
                    ValueRaw::Int64(_) => data_types.push(Int64),
                    ValueRaw::Float64(_) => data_types.push(Float64),
                    ValueRaw::Utf8(_) => data_types.push(Utf8),
                    ValueRaw::Boolean(_) => data_types.push(Boolean),
                    ValueRaw::Date(_) => data_types.push(Date32),
                    ValueRaw::DateTime(x) => data_types.push(Timestamp(
                        Second,
                        Some(x.clone().format("%Y-%m-%d %H:%M:%S").to_string()),
                    )),
                }
            };

            let mut all_types = Vec::new();
    // get data types provided by user and panic if they are not compatible use vctr rules
    for i in 0..data_types.len() - 1 {
        if i < 1 {
            all_types.push(data_types[i].clone());
        }
        match (&data_types[i], &data_types[i + 1]) {
            (Timestamp(Second, Some(_)), Boolean) => {
                panic!("Can't combine {} and {}.", data_types[i], data_types[i + 1])
            }
            (Timestamp(Second, Some(_)), Int64) => {
                panic!("Can't combine {} and {}.", data_types[i], data_types[i + 1])
            }
            (Timestamp(Second, Some(_)), Float64) => {
                panic!("Can't combine {} and {}.", data_types[i], data_types[i + 1])
            }
            (Timestamp(Second, Some(_)), Utf8) => {
                panic!("Can't combine {} and {}.", data_types[i], data_types[i + 1])
            }
            (Timestamp(Second, Some(_)), Date32) => {
                all_types.push(Timestamp(Second, Some(data_types[i].to_string())));
            }
            (Timestamp(Second, Some(_)), Timestamp(Second, Some(_))) => {
                all_types.push(Timestamp(Second, Some(data_types[i].to_string())));
            }
            (Boolean, Timestamp(Second, Some(_))) => {
                panic!("Can't combine {} and {}.", data_types[i], data_types[i + 1])
            }
            (Int64, Timestamp(Second, Some(_))) => {
                panic!("Can't combine {} and {}.", data_types[i], data_types[i + 1])
            }
            (Float64, Timestamp(Second, Some(_))) => {
                panic!("Can't combine {} and {}.", data_types[i], data_types[i + 1])
            }
            (Utf8, Timestamp(Second, Some(_))) => {
                panic!("Can't combine {} and {}.", data_types[i], data_types[i + 1])
            }
            (Date32, Timestamp(Second, Some(_))) => {
                all_types.push(Timestamp(Second, Some(data_types[i].to_string())))
            }
            // Date32
            (Date32, Boolean) => {
                panic!("Can't combine {} and {}.", data_types[i], data_types[i + 1])
            }
            (Date32, Int64) => {
                panic!("Can't combine {} and {}.", data_types[i], data_types[i + 1])
            }
            (Date32, Float64) => {
                panic!("Can't combine {} and {}.", data_types[i], data_types[i + 1])
            }
            (Date32, Utf8) => {
                panic!("Can't combine {} and {}.", data_types[i], data_types[i + 1])
            }
            (Date32, Date32) => {
                all_types.push(Date32);
            }
            (Boolean, Date32) => {
                panic!("Can't combine {} and {}.", data_types[i], data_types[i + 1])
            }
            (Int64, Date32) => {
                panic!("Can't combine {} and {}.", data_types[i], data_types[i + 1])
            }
            (Float64, Date32) => {
                panic!("Can't combine {} and {}.", data_types[i], data_types[i + 1])
            }
            (Utf8, Date32) => {
                panic!("Can't combine {} and {}.", data_types[i], data_types[i + 1])
            }
            // char
            (Utf8, Boolean) => {
                panic!("Can't combine {} and {}.", data_types[i], data_types[i + 1])
            }
            (Utf8, Int64) => {
                panic!("Can't combine {} and {}.", data_types[i], data_types[i + 1])
            }
            (Utf8, Float64) => {
                panic!("Can't combine {} and {}.", data_types[i], data_types[i + 1])
            }
            (Utf8, Utf8) => {
                all_types.push(Utf8);
            }
            (Boolean, Utf8) => {
                panic!("Can't combine {} and {}.", data_types[i], data_types[i + 1])
            }
            (Int64, Utf8) => {
                panic!("Can't combine {} and {}.", data_types[i], data_types[i + 1])
            }
            (Float64, Utf8) => {
                panic!("Can't combine {} and {}.", data_types[i], data_types[i + 1])
            }
            // dbl
            (Float64, Boolean) => {
                all_types.push(Float64);
            }
            (Float64, Int64) => {
                all_types.push(Float64);
            }
            (Float64, Float64) => {
                all_types.push(Float64);
            }
            (Boolean, Float64) => {
                all_types.push(Float64);
            }
            (Int64, Float64) => {
                all_types.push(Float64);
            }
            // int
            (Int64, Boolean) => {
                all_types.push(Int64);
            }
            (Int64, Int64) => {
                all_types.push(Int64);
            }
            (Boolean, Int64) => {
                all_types.push(Int64);
            }
            // bool
            (Boolean, Boolean) => {
                all_types.push(Boolean);
            }
            // Null
            (Null, Null) => {
                all_types.push(Null);
            }
            (Null, Boolean) => {
                all_types.push(Boolean);
            }
            (Null, Int64) => {
                all_types.push(Int64);
            }
            (Null, Float64) => {
                data_types.push(Float64);
            }
            (Null, Utf8) => {
                all_types.push(Utf8);
            }
            (Null, Date32) => {
                all_types.push(Date32);
            }
            (Null, Timestamp(Second, Some(_))) => {
                all_types.push(Timestamp(Second, Some(data_types[i + 1].to_string())));
            }
            (Boolean, Null) => {
                print!("found it");
                all_types.push(Boolean);
            }
            (Int64, Null) => {
                all_types.push(Int64);
            }
            (Float64, Null) => {
                all_types.push(Float64);
            }
            (Utf8, Null) => {
                all_types.push(Utf8);
            }
            (Date32, Null) => {
                all_types.push(Date32);
            }
            (Timestamp(Second, Some(_)), Null) => {
                all_types.push(Timestamp(Second, Some(data_types[i + 1].to_string())));
            }
            _ => {
                panic!("Can't combine {} and {}. The only supported types are Boolean, Int64, Float64, UTF, Date32, Timestamp", data_types[i], data_types[i+1])
            }
        }
    };
    let prototype: arrow::datatypes::DataType = if all_types.contains(&Float64) {
        Float64
    } else if all_types.contains(&Int64) & !all_types.contains(&Float64) {
        Int64
    } else if all_types.contains(&Boolean)
        & !all_types.contains(&Float64)
        & !all_types.contains(&Int64)
    {
        Boolean
    } else if all_types.contains(&Utf8) {
        Utf8
    } else if all_types.contains(&Date32)
        & !all_types
            .iter()
            .any(|elem| matches!(elem, Timestamp(Second, Some(_))))
    {
        Date32
    } else if let Some(elem) = all_types
        .iter()
        .find(|elem| matches!(elem, Timestamp(Second, Some(_))))
    {
        Timestamp(Second, Some(elem.to_string()))
    } else {
        Null
    };

    // if `prototype` is a Float64, cast the vector `v` to Float64Array

    let casted: Vec<ArrayProtype> = v.into();


    // let casted_array: ArrayProtype = match prototype {
    //     Float64 =>    v.into_iter().map(|x| x.into()).collect::<Float64Array>(),
    //     Int64 =>     v.into_iter().map(|x| x.into()).collect::<Int64Array>(),
    //     Boolean =>   v.into_iter().map(|x| x.into()).collect::<BooleanArray>(),
    //     Utf8 =>      v.into_iter().map(|x| x.into()).collect::<StringArray>(),
    //     Date32 =>    v.into_iter().map(|x| x.into()).collect::<Date32Array>(),
    //     Timestamp(Second, Some(_)) => v.into_iter().map(|x| x.into()).collect::<TimestampSecondArray>(),
    //     _ => panic!("Can't cast to this type."),

    // cast the vector `v` to type in `prototype`
    // let casted_array: ArrayProtype = match prototype {
    //     Float64 =>    v.into_iter().map(|x| x.into()).collect::<Float64Array>(),
    //     Int64 =>     v.into_iter().map(|x| x.into()).collect::<Int64Array>(),
    //     Boolean =>   v.into_iter().map(|x| x.into()).collect::<BooleanArray>(),
    //     Utf8 =>      v.into_iter().map(|x| x.into()).collect::<StringArray>(),
    //     Date32 =>    v.into_iter().map(|x| x.into()).collect::<Date32Array>(),
    //     Timestamp(Second, Some(_)) => v.into_iter().map(|x| x.into()).collect::<TimestampSecondArray>(),
    //     _ => panic!("Can't cast to this type."),
    // };
    // casted_array

}
}
}

fn main() {
    //let mixed_vec: Vec<ValueRaw> = vec_c!("2021-03-25 23:30:10", "2021-03-25", Null);
    //let mixed_vec: Vec<ValueRaw> = vec_c!(false, Null);
    let mixed_vec = vec_c!(false, 6, 8.9, Null);
}
