// when writing new functions call this file main.rs
// when it is cleaned up convert it back to lib.rs
// impliment a format for Record batch
#![doc(html_favicon_url = "https://storage.googleapis.com/data_xvzf/ylr_favicon.ico")]
#![doc(html_logo_url = "https://storage.googleapis.com/data_xvzf/ylr_small_docs_logo.png")]
#![doc = include_str!("../README.md")]
#![warn(missing_docs)]
use arrow::array::{Array, StringArray};
use arrow::compute::cast;
use arrow::datatypes::DataType::{Boolean, Date32, Float64, Int64, List, Timestamp, Utf8};
use arrow::record_batch::RecordBatch;
use arrow_csv::reader;
use core::str;
use crossterm::terminal::size;
use futures_util::StreamExt;
use indicatif::{ProgressBar, ProgressStyle};
use lazy_static::lazy_static;
use owo_colors::OwoColorize;
use regex::Regex;
use std::cmp::min;
use std::cmp::Ordering;
use std::fmt;
use std::fs::remove_file;
use std::fs::File;
use std::io::prelude::*;
use std::path::PathBuf;
use std::str::FromStr;
use std::sync::Arc;
use tempfile::tempdir;
use unicode_truncate::UnicodeTruncateStr;
// import intarray
use arrow::array::Int64Array;
use arrow::datatypes::Field;
use arrow::datatypes::Schema;
// import DataType
use arrow::datatypes::DataType;
use std::ffi::OsStr;
use std::path::Path;
use tempfile::TempDir;
use url::Url;
#[tokio::main]
async fn download_data(url_path: &str, temp_file: PathBuf) {
    let mut tmp_file = File::create(temp_file).expect("failed to create temp file");
    let url = url_path;
    let response = reqwest::get(url);
    let mut stream = response.await.expect("error on streaming").bytes_stream();
    let total_size = reqwest::get(url)
        .await
        .expect("asdfd")
        .content_length()
        .ok_or(format!("Failed to get content length from '{}'", &url))
        .unwrap();
    let mut downloaded: u64 = 0;

    // Indicatif setup
    let pb = ProgressBar::new(total_size);
    pb.set_style(
        ProgressStyle::default_bar()
            .template("{msg} {spinner:.yellow} {bytes}/{total_bytes} ETA: {eta}")
            .unwrap(),
    );
    pb.set_message(format!("Fetching Data..."));
    while let Some(item) = stream.next().await {
        let chunk = item.expect("Error while downloading file");
        tmp_file
            .write_all(&chunk)
            .expect("error on writing to file");
        let new = min(downloaded + (chunk.len() as u64), total_size);
        downloaded = new;
        pb.set_position(new);
    }
    pb.finish_with_message(format!("🐶 Download Success!"));
}

mod pillar {
    use super::*;

    pub struct PillarRecordBatch {
        table: RecordBatch,
        is_no_row_numbering: bool,
        is_tty: bool,
        is_force_all_rows: bool,
        is_force_all_columns: bool,
        num_rows_overide: usize,
        lower_column_width: usize,
        upper_column_width: usize,
        sigfig: i64,
    }

    impl fmt::Display for PillarRecordBatch {
        fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
            let term_tuple: (u16, u16) = size().unwrap();
            let is_tty = self.is_tty;
            let is_force_color = false;
            let is_no_row_numbering = self.is_no_row_numbering;
            let cols: usize = self.table.num_columns();
            let rows_in_file: usize = self.table.num_rows() + 1;
            let rows: usize = self.num_rows_overide;
            let is_force_all_rows = self.is_force_all_rows;
            let lower_column_width = self.lower_column_width;
            let upper_column_width = self.upper_column_width;
            let sigfig = self.sigfig;

            pub fn is_number(text: String) -> bool {
                is_integer(text.clone()) || is_double(text.clone())
            }

            pub fn is_negative_number(text: String) -> bool {
                lazy_static! {
                    static ref R: Regex = Regex::new(r"^\s*-[0-9]*.?[0-9]*\s*$").unwrap();
                }
                R.is_match(text.as_str())
            }

            pub fn is_integer(text: String) -> bool {
                //let integer = "5";
                lazy_static! {
                    static ref R: Regex = Regex::new(r"^\s*([+-]?[1-9][0-9]*|0)\s*$").unwrap();
                }
                R.is_match(text.as_str())
            }

            let color_option = 1;
            let extend_width_length_option = false;
            let meta_color: [u8; 3] = [232, 196, 96];
            let header_color: [u8; 3] = [232, 196, 96];
            let std_color: [u8; 3] = [198, 192, 168];
            let na_color: [u8; 3] = [191, 97, 106];
            let neg_num_color: [u8; 3] = [219, 67, 75];
            let (meta_color, header_color, std_color, na_color, neg_num_color) = match color_option
            {
                1 => (meta_color, header_color, std_color, na_color, neg_num_color),
                _ => (meta_color, header_color, std_color, na_color, neg_num_color),
            };
            fn get_num_cols_to_print(
                cols: usize,
                vp: Vec<Vec<String>>,
                term_tuple: (u16, u16),
            ) -> usize {
                let mut last = 0;
                let mut j = format!("{: >6}  ", "");
                for col in 0..cols {
                    let text = vp[0].get(col).unwrap().to_string();
                    j.push_str(&text);
                    let total_width = j.chars().count();
                    let term_width = term_tuple.0 as usize;
                    if total_width > term_width {
                        break;
                    }
                    last = col + 1;
                }
                last
            }

            let rows_remaining: usize = match is_force_all_rows {
                true => 0,
                false => match rows_in_file.cmp(&rows) {
                    Ordering::Less => 0,
                    Ordering::Greater => rows_in_file - rows,
                    Ordering::Equal => 0,
                },
            };

            let rows = match is_force_all_rows {
                true => rows_in_file,
                false => match rows_in_file.cmp(&rows) {
                    Ordering::Less => rows_in_file,  // rows_in_file - rows < 0
                    Ordering::Greater => rows,       // rows_in_file - rows > 0
                    Ordering::Equal => rows_in_file, // rows_in_file - rows == 0
                },
            };

            let ellipsis = '\u{2026}'.to_string();
            let row_remaining_text: String =
                format!("{} with {} more rows", ellipsis, rows_remaining);
            let record_batch_schema_clone = self.table.schema().clone();

            let data_types: Vec<String> = record_batch_schema_clone
                .fields()
                .iter()
                .map(|field| {
                    let data_type = field.data_type();
                    match data_type {
                        Boolean => "<bool>".to_string(),
                        Int64 => "<int>".to_string(),
                        Float64 => "<dbl>".to_string(),
                        Utf8 => "<chr>".to_string(),
                        List(_) => "<list>".to_string(),
                        Date32 => "<date>".to_string(),
                        Timestamp(_, _) => "<ts>".to_string(),
                        _ => "<_>".to_string(),
                    }
                })
                .collect();

            let record_batch_vec_vec_string: Vec<Vec<String>> = self
                .table
                .columns()
                .iter()
                .map(|array| {
                    let array = cast(array, &Utf8).unwrap();
                    let string_array = array.as_any().downcast_ref::<StringArray>().unwrap();
                    string_array
                        .iter()
                        .map(|x| x.unwrap_or_default().to_owned())
                        .collect::<Vec<String>>()
                })
                .collect::<Vec<Vec<String>>>();

            let mut v = record_batch_vec_vec_string.clone();

            // insert data_types[i] to the beginning of each vector[i] in v
            v.iter_mut()
                .enumerate()
                .for_each(|(i, col)| col.insert(0, data_types[i].to_string()));
            // insert a vector of column names to the beginning of each vector in vf_mut
            let column_names = record_batch_schema_clone
                .fields()
                .iter()
                .map(|field| field.name().to_string())
                .collect::<Vec<String>>();
            // insert column_names[i] to the beginning of each vector[i] in v
            v.iter_mut()
                .enumerate()
                .for_each(|(i, col)| col.insert(0, column_names[i].to_string()));

            // for each vector in v format the strings
            let vf_mut = v
                .into_iter()
                .map(|col| format_strings(col, lower_column_width, upper_column_width, sigfig))
                .collect::<Vec<Vec<String>>>();

            // for each vector convert the columns to rows in vf_mut

            let mut vp: Vec<Vec<String>> = Vec::new();
            dbg!(rows);
            for r in 0..rows + 1 {
                // rows + 2 because we have to account for the header and the data types
                let row = vf_mut.iter().map(|col| col[r].to_string()).collect();
                vp.push(row);
            }

            let num_cols_to_print = if extend_width_length_option {
                cols
            } else {
                get_num_cols_to_print(cols, vf_mut.clone(), term_tuple)
            };

            pub struct DecimalSplits {
                pub val: f64,
                pub sigfig: i64,
            }

            impl DecimalSplits {
                pub fn value(&self) -> f64 {
                    self.val
                }
                pub fn sig_fig(&self) -> i64 {
                    self.sigfig
                }
                pub fn neg(&self) -> bool {
                    is_neg(self.val)
                }
                pub fn lhs(&self) -> f64 {
                    get_lhs(self.val)
                }
                pub fn rhs(&self) -> f64 {
                    get_rhs(self.val)
                }
                //pub fn dec(&self) -> bool {
                //    is_decimal(self.val)
                //}
                pub fn final_string(&self) -> String {
                    get_final_string(
                        self.value(),
                        self.lhs(),
                        self.rhs(),
                        self.neg(),
                        self.sig_fig(),
                    )
                }
            }

            fn is_neg(x: f64) -> bool {
                x < 0.0
            }

            fn get_lhs(x: f64) -> f64 {
                x.trunc().abs()
            }

            fn get_rhs(x: f64) -> f64 {
                let xint = x.trunc();
                let frac = x - xint;
                frac.abs()
                //let s = format!("{:.12}", frac.abs()); //The 10 is arbitraty, but this condition puts a cap on sigfig size
                //let f: f64 = s.parse::<f64>().unwrap();
                //f
            }

            pub fn get_final_string(x: f64, lhs: f64, rhs: f64, neg: bool, sigfig: i64) -> String {
                if lhs.abs() + rhs.abs() == 0.0 {
                    "0".to_string()
                } else if lhs == 0.0 {
                    //n = ((floor(log10(abs(x))) + 1 - sigfig)
                    //r =(10^n) * round(x / (10^n))
                    let n = x.abs().log10().floor() + 1.0 - sigfig as f64;
                    let r: f64 = 10f64.powf(n) * ((x / 10f64.powf(n)).round());
                    let tmp_string = r.to_string();
                    if tmp_string.len() > 13 {
                        // 13 is arbitraty. There may be a more general solution here!
                        // Problem: debug val: 0.0001 => final_string: "0.00009999999999999999"
                        let j = (x.abs().log10().floor()).abs() as usize;
                        if j >= sigfig as usize {
                            // long tail sigfigs
                            // 0.0001
                            // 0.001
                            let w = (x.abs().log10().floor()).abs() as usize;
                            let fstring = format!("{:.w$}", r, w = w);
                            fstring
                        } else {
                            // standard lhs only sigs
                            //-0.9527948462413667 -> -0.953
                            let fstring = format!("{:.w$}", r, w = (sigfig as usize));
                            fstring
                        }
                    } else {
                        //println!("{:?}", tmp_string);
                        tmp_string
                    }
                } else if lhs.log10() + 1.0 >= sigfig as f64 {
                    if rhs > 0.0 {
                        let total = lhs + rhs;
                        let total_string = total.to_string();
                        let total_clone = total_string.clone();
                        let split = total_clone.split('.');
                        let vec: Vec<&str> = split.collect();
                        let len_to_take = vec[0].len() + 1; // lhs + point
                        if neg {
                            //concatonate:
                            //(-)
                            //(lhs)
                            //(point)
                            //(-123.45 -> -123.)
                            let pos_string = (total_string[..len_to_take]).to_string();
                            let neg_string = "-".to_string();
                            [neg_string, pos_string].join("")
                        } else {
                            //concatonate:
                            //(lhs)
                            //(point)
                            //(123.45 -> 123.)
                            total_string[..len_to_take].to_string()
                        }
                    } else if neg {
                        //concatonate:
                        //(-)
                        //(lhs)
                        //(-1234.0 -> -1234)
                        let total = lhs + rhs;
                        let total_string = total.to_string();
                        let total_clone = total_string.clone();
                        let split = total_clone.split('.');
                        let vec: Vec<&str> = split.collect();
                        let len_to_take = vec[0].len(); // lhs
                        let pos_string = (total_string[..len_to_take]).to_string();
                        let neg_string = "-".to_string();
                        [neg_string, pos_string].join("")
                    } else {
                        //concatonate:
                        //(lhs)
                        //(1234.0 -> 1234)
                        //(100.0 -> 100)
                        //let total = lhs + rhs;
                        //let total_string = total.to_string();
                        let total_string = x.to_string();
                        let total_clone = total_string.clone();
                        let split = total_clone.split('.');
                        let vec: Vec<&str> = split.collect();
                        let len_to_take = vec[0].len(); // lhs
                        total_string[..len_to_take].to_string()
                    }
                } else if rhs == 0.0 {
                    //concatonate:
                    //(lhs)
                    //(point)
                    //+ sigfig - log10(lhs) from rhs
                    let total_string = x.to_string();
                    let total_clone = total_string.clone();
                    let split = total_clone.split('.');
                    let vec: Vec<&str> = split.collect();
                    let len_to_take_lhs = vec[0].len(); // point -> +1 to sigfig
                    total_string[..len_to_take_lhs].to_string()
                } else if neg {
                    //concatonate:
                    //(-)
                    //(lhs)
                    //(point)
                    //+ sigfig - log10(lhs) from rhs
                    //(-12.345 -> -12.3)
                    //(-1.2345 -> -1.23)
                    // need a rhs arguments here
                    //let total = lhs + rhs;
                    //let total_string = total.to_string();
                    let w: usize = (sigfig as usize) - 1;
                    let x = format!("{:.w$}", x, w = w);
                    let total_string = x;
                    let total_clone = total_string.clone();
                    let split = total_clone.split('.');
                    let vec: Vec<&str> = split.collect();
                    let len_to_take_lhs = vec[0].len(); // point -> +1 to sigfig
                                                        // The plus one at the end stands for the '.' character as lhs doesn't include it
                    let len_to_take_rhs =
                        std::cmp::min((sigfig as usize) - len_to_take_lhs, vec[1].len()) + 1;
                    let len_to_take = len_to_take_lhs + len_to_take_rhs + 1;
                    //println!("x: {:?}", x);
                    total_string[..len_to_take].to_string()
                } else {
                    //concatonate:
                    //(lhs)
                    //(point)
                    //+ sigfig - log10(lhs) from rhs
                    //(12.345 -> 12.3)
                    //(1.2345 -> 1.23)
                    // need a rhs arguments here
                    //let total = lhs + rhs;
                    //let total_string = total.to_string();
                    let w: usize = (sigfig as usize) - 1;
                    let x = format!("{:.w$}", x, w = w);
                    let total_string = x;
                    let total_clone = total_string.clone();
                    let split = total_clone.split('.');
                    let vec: Vec<&str> = split.collect();
                    let len_to_take_lhs = vec[0].len(); // point -> +1 to sigfig
                    let len_to_take_rhs = ((sigfig + 1) as usize) - len_to_take_lhs;
                    let len_to_take = len_to_take_lhs + len_to_take_rhs;

                    if len_to_take >= total_string.len() {
                        total_string
                    } else {
                        total_string[..len_to_take].to_string()
                    }
                }
            }

            pub fn format_if_na(text: String) -> String {
                // todo add repeat strings for NA
                let missing_string_value = "NA";
                let string = if is_na(text.clone()) {
                    missing_string_value
                } else {
                    text.as_str()
                };
                string.to_string()
            }

            pub fn is_double(text: String) -> bool {
                f64::from_str(text.trim()).is_ok()
            }

            pub fn is_na(text: String) -> bool {
                lazy_static! {
                    static ref R: Regex = Regex::new(
                        r"^$|^(?:N(?:(?:(?:one|AN|a[Nn]|/A)|[Aa])|ull)|n(?:ull|an?|/a?)|(?:missing))$"
                    )
                    .unwrap();
                }
                R.is_match(text.as_str())
            }

            pub fn is_na_string_padded(text: String) -> bool {
                lazy_static! {
                    static ref R: Regex = Regex::new(
                        r"^$|(^|\s)(?:N(?:(?:(?:AN|a[Nn]|/A)|[Aa])|ull)|n(?:ull|an?|/a?)|(?:missing))\s*$"
                    )
                    .unwrap();
                }
                R.is_match(text.as_str())
            }

            pub fn format_if_num(text: String, sigfig: i64) -> String {
                if let Ok(val) = text.parse::<f64>() {
                    DecimalSplits { val, sigfig }.final_string()
                } else {
                    text.to_string()
                }
            }

            // format strings
            pub fn format_strings(
                vec_col: Vec<String>,
                lower_column_width: usize,
                upper_column_width: usize,
                sigfig: i64,
            ) -> Vec<String> {
                let ellipsis = '\u{2026}';

                let strings_and_fracts: Vec<(String, usize, usize)> = vec_col
                    .iter()
                    .map(|string| format_if_na(string.clone()))
                    .map(|string| format_if_num(string.clone(), sigfig))
                    .map(|string| {
                        // the string, and the length of its fractional digits if any
                        let (lhs, rhs) = if is_double(string.clone()) {
                            let mut split = string.split('.');
                            (
                                split.next().map(|lhs| lhs.len()).unwrap_or_default(),
                                split.next().map(|rhs| rhs.len()).unwrap_or_default(),
                            )
                        } else {
                            (0, 0)
                        };
                        (string, lhs, rhs)
                    })
                    .collect();

                let max_fract: usize = strings_and_fracts
                    .iter()
                    .map(|(_, _, fract)| *fract)
                    .max()
                    .unwrap_or_default();
                let max_whole: usize = strings_and_fracts
                    .iter()
                    .map(|(_, whole, _)| *whole)
                    .max()
                    .unwrap_or_default();

                let strings_and_widths: Vec<(String, usize)> = strings_and_fracts
                    .into_iter()
                    .map(|(mut string, whole, fract)| {
                        if max_fract > 0 && is_double(string.clone()) {
                            if whole < max_whole {
                                let mut s = String::new();
                                s.push_str(&" ".repeat(max_whole - whole));
                                s.push_str(&string);
                                string = s;
                            }

                            string.push_str(&" ".repeat(max_fract - fract));
                        } else if max_fract > 0 && is_na(string.clone()) {
                            if 2 < max_whole {
                                let mut s = String::new();
                                s.push_str(&" ".repeat(max_whole - 2));
                                s.push_str(&string);
                                string = s;
                            }

                            string.push_str(&" ".repeat(max_fract - fract));
                        }
                        let len = string.chars().count();
                        // the string and its length
                        (string, len)
                    })
                    .collect();

                let max_width: usize = strings_and_widths
                    .iter()
                    .map(|(_, width)| *width)
                    .max()
                    .unwrap_or_default()
                    .clamp(lower_column_width, upper_column_width);

                strings_and_widths
                    .into_iter()
                    .map(|(string, len)| {
                        if len > max_width {
                            let (rv, _) = string.unicode_truncate(max_width - 1);
                            let spacer: &str = " ";
                            let string_and_ellipses =
                                [rv.to_string(), ellipsis.to_string()].join("");
                            [string_and_ellipses, spacer.to_string()].join("")
                        } else {
                            let add_space = max_width - len + 1;
                            let borrowed_string: &str = &" ".repeat(add_space);
                            [string, "".to_string()].join(borrowed_string)
                        }
                    })
                    .collect()
            }

            //=======ylr dim: Row x Col=======
            let _ = match writeln!(f) {
                Ok(_) => Ok(()),
                Err(e) => Err(e),
            };
            let meta_text: &str = "🐶 A tibble dim:";
            let div: &str = "x";
            let _ = match write!(f, "{: >3}  ", "") {
                Ok(_) => Ok(()),
                Err(x) => Err(x),
            };
            let _ = match write!(
                f,
                "{} {} {} {}",
                meta_text.truecolor(header_color[0], header_color[1], header_color[2]), // tv dim:
                (rows_in_file - 1).truecolor(header_color[0], header_color[1], header_color[2]), // rows
                div.truecolor(header_color[0], header_color[1], header_color[2]), // x
                (cols).truecolor(header_color[0], header_color[1], header_color[2]), // cols
            ) {
                Ok(_) => Ok(()),
                Err(x) => Err(x),
            };
            let _ = match writeln!(f) {
                Ok(_) => Ok(()),
                Err(e) => Err(e),
            };
            //===col names===
            let _ = match write!(f, "{: >6}  ", "") {
                Ok(_) => Ok(()),
                Err(e) => Err(e),
            };
            for col in 0..num_cols_to_print {
                let text = vp[0].get(col).unwrap().to_string();
                let _ = match write!(
                    f,
                    "{}",
                    text.truecolor(header_color[0], header_color[1], header_color[2])
                        .bold()
                ) {
                    Ok(_) => Ok(()),
                    Err(e) => Err(e),
                };
            }
            let _ = match writeln!(f) {
                Ok(_) => Ok(()),
                Err(e) => Err(e),
            };
            //===data types===
            let _ = match write!(f, "{: >6}  ", "") {
                Ok(_) => Ok(()),
                Err(e) => Err(e),
            };
            for col in 0..num_cols_to_print {
                let text = vp[1].get(col).unwrap().to_string();
                if is_tty || is_force_color {
                    let _ = match write!(
                        f,
                        "{}",
                        text.truecolor(meta_color[0], meta_color[1], meta_color[2])
                            .bold()
                    ) {
                        Ok(_) => Ok(()),
                        Err(e) => Err(e),
                    };
                } else {
                    let _ = match write!(f, "{}", text) {
                        Ok(_) => Ok(()),
                        Err(e) => Err(e),
                    };
                }
            }
            let _ = match writeln!(f) {
                Ok(_) => Ok(()),
                Err(e) => Err(e),
            };
            //===data main body===
            vp.iter()
                .enumerate()
                //.take(rows + 20)
                .skip(2)
                .for_each(|(row_index, row)| {
                    if is_tty || is_force_color {
                        if is_no_row_numbering {
                            let _ = match write!(
                                f,
                                "{: >6}  ",
                                "".truecolor(meta_color[0], meta_color[1], meta_color[2]) // this prints the row number
                            ) {
                                Ok(_) => Ok(()),
                                Err(e) => Err(e),
                            };
                        } else {
                            let _ = match write!(
                                f,
                                "{: >6}  ",
                                (row_index - 1).truecolor(
                                    meta_color[0],
                                    meta_color[1],
                                    meta_color[2]
                                ) // this prints the row number
                            ) {
                                Ok(_) => Ok(()),
                                Err(e) => Err(e),
                            };
                        }
                    } else if is_no_row_numbering {
                        let _ = match write!(
                            f,
                            "{: >6}  ",
                            "" // this prints the row number
                        ) {
                            Ok(_) => Ok(()),
                            Err(e) => Err(e),
                        };
                    } else {
                        let _ = match write!(
                            f,
                            "{: >6}  ",
                            "" // this prints the row number
                        ) {
                            Ok(_) => Ok(()),
                            Err(e) => Err(e),
                        };
                    }
                    row.iter().take(num_cols_to_print).for_each(|col| {
                        if is_tty || is_force_color {
                            let _ = match write!(
                                f,
                                "{}",
                                if is_na_string_padded(col.clone()) {
                                    col.truecolor(na_color[0], na_color[1], na_color[2])
                                } else if is_number(col.clone()) && is_negative_number(col.clone())
                                {
                                    col.truecolor(
                                        neg_num_color[0],
                                        neg_num_color[1],
                                        neg_num_color[2],
                                    )
                                } else {
                                    col.truecolor(std_color[0], std_color[1], std_color[2])
                                }
                            ) {
                                Ok(_) => Ok(()),
                                Err(e) => Err(e),
                            };
                        } else {
                            let _ = match write!(f, "{}", col) {
                                Ok(_) => Ok(()),
                                Err(e) => Err(e),
                            };
                        }
                    });
                    let _ = match writeln!(f) {
                        Ok(_) => Ok(()),
                        Err(e) => Err(e),
                    };
                });
            //===footer===
            if rows_remaining > 0 || (cols - num_cols_to_print) > 0 {
                let _ = match write!(f, "{: >6}  ", "") {
                    Ok(_) => Ok(()),
                    Err(e) => Err(e),
                };
                if is_tty || is_force_color {
                    let _ = match write!(
                        f,
                        "{}",
                        row_remaining_text.truecolor(meta_color[0], meta_color[1], meta_color[2])
                    ) {
                        Ok(_) => Ok(()),
                        Err(e) => Err(e),
                    };
                } else {
                    let _ = match write!(f, "{}", row_remaining_text) {
                        Ok(_) => Ok(()),
                        Err(e) => Err(e),
                    };
                }
                let extra_cols_to_mention = num_cols_to_print;
                let remainder_cols = cols - extra_cols_to_mention;
                if extra_cols_to_mention < cols {
                    let meta_text_and = "and";
                    let meta_text_var = "more variables";
                    let meta_text_comma = ",";
                    let meta_text_colon = ":";
                    if is_tty || is_force_color {
                        let _ = match write!(
                            f,
                            " {} {} {}{}",
                            meta_text_and.truecolor(meta_color[0], meta_color[1], meta_color[2]),
                            remainder_cols.truecolor(meta_color[0], meta_color[1], meta_color[2]),
                            meta_text_var.truecolor(meta_color[0], meta_color[1], meta_color[2]),
                            meta_text_colon.truecolor(meta_color[0], meta_color[1], meta_color[2])
                        ) {
                            Ok(_) => Ok(()),
                            Err(e) => Err(e),
                        };
                    } else {
                        let _ = match write!(
                            f,
                            " {} {} {}{}",
                            meta_text_and, remainder_cols, meta_text_var, meta_text_colon
                        ) {
                            Ok(_) => Ok(()),
                            Err(e) => Err(e),
                        };
                    }
                    for col in extra_cols_to_mention..cols {
                        let text = column_names[col].clone();
                        if is_tty || is_force_color {
                            let _ = match write!(
                                f,
                                " {}",
                                text.truecolor(meta_color[0], meta_color[1], meta_color[2])
                            ) {
                                Ok(_) => Ok(()),
                                Err(e) => Err(e),
                            };
                        } else {
                            let _ = match write!(f, " {}", text) {
                                Ok(_) => Ok(()),
                                Err(e) => Err(e),
                            };
                        }

                        // The last column mentioned in foot should not be followed by a comma
                        if col + 1 < cols {
                            if is_tty || is_force_color {
                                let _ = match write!(
                                    f,
                                    "{}",
                                    meta_text_comma.truecolor(
                                        meta_color[0],
                                        meta_color[1],
                                        meta_color[2]
                                    )
                                ) {
                                    Ok(_) => Ok(()),
                                    Err(e) => Err(e),
                                };
                            } else {
                                let _ = match write!(f, "{}", meta_text_comma) {
                                    Ok(_) => Ok(()),
                                    Err(e) => Err(e),
                                };
                            }
                        }
                    } // end extra cols mentioned in footer
                }
            }
            let _ = match writeln!(f) {
                Ok(_) => Ok(()),
                Err(e) => Err(e),
            };
            Ok(())
        }
    }

    pub fn to_pillar(RecordBatch: RecordBatch) -> PillarRecordBatch {
        PillarRecordBatch {
            table: RecordBatch,
            is_no_row_numbering: false,
            is_force_all_columns: false,
            is_tty: true,
            is_force_all_rows: false,
            num_rows_overide: 30,
            lower_column_width: 5,
            upper_column_width: 20,
            sigfig: 3,
        }
    }
}

/// A minimal port of [readr 2.1.4](https://readr.tidyverse.org/) by Posit
pub mod readr {
    use super::*;
    use arrow::ipc::Decimal;
    use pillar::to_pillar;
    use pillar::PillarRecordBatch;

    pub fn read_delim(file: &str, delim: &str) -> PillarRecordBatch {
        //! `read_csv()` and `read_tsv()` are special cases of the more general `read_delim()`.
        //! They're useful for reading the most common types of flat file data, comma separated values and tab separated values, respectively.
        //! read_csv2() uses ; for the field separator and , for the decimal point. This format is common in some European countries.
        //! # Arguments
        //! #### `file`
        //!
        //! Either a path to a file, a connection, or literal data (either a single string or a raw vector).
        //! Files ending in `.gz`, `.bz2`, `.xz`, or `.zip` will be automatically uncompressed. Files starting with `http://`, `https://`, `ftp://`, or `ftps://` will be automatically downloaded. Remote `gz` files can also be automatically downloaded and decompressed.
        //! Literal data is most useful for examples and tests. To be recognised as literal data, the input must be either wrapped with I(), be a string containing at least one new line, or be a vector containing at least one string with a new line.
        //!
        //! #### `delim`
        //!
        //! Single character used to separate fields within a record.
        //!
        //! #### `escape_backslash`
        //!
        //! Does the file use backslashes to escape special characters? This is more general than escape_double as backslashes can be used to escape the delimiter character, the quote character, or to add special characters like \\n.
        //!
        //! #### `escape_double`
        //! Does the file escape quotes by doubling them? i.e. If this option is TRUE, the value """" represents a single quote, \".
        //!
        //! #### col_names
        //! Either TRUE, FALSE or a character vector of column names.
        //!
        //! If TRUE, the first row of the input will be used as the column names, and will not be included in the data frame. If FALSE, column names will be generated automatically: X1, X2, X3 etc.
        //!
        //! If col_names is a character vector, the values will be used as the names of the columns, and the first row of the input will be read into the first row of the output data frame.
        //!
        //! Missing (NA) column names will generate a warning, and be filled in with dummy names ...1, ...2 etc. Duplicate column names will generate a warning and be made unique, see name_repair to control how this is done.
        //!
        //! #### col_types
        //! One of NULL, a cols() specification, or a string. See vignette("readr") for more details.
        //!
        //! If NULL, all column types will be inferred from guess_max rows of the input, interspersed throughout the file. This is convenient (and fast), but not robust. If the guessed types are wrong, you'll need to increase guess_max or supply the correct types yourself.
        //!
        //! Column specifications created by list() or cols() must contain one column specification for each column. If you only want to read a subset of the columns, use cols_only().
        //!
        //! Alternatively, you can use a compact string representation where each character represents one column:
        //!
        //! c = character
        //!
        //! i = integer
        //!
        //! n = number
        //!
        //! d = double
        //!
        //! l = logical
        //!
        //! f = factor
        //!
        //! D = date
        //!
        //! T = date time
        //!
        //! t = time
        //!
        //! ? = guess
        //!
        //! _ or - = skip
        //!
        //! By default, reading a file without a column specification will print a message showing what readr guessed they were. To remove this message, set show_col_types = FALSE or set `options(readr.show_col_types = FALSE).
        //!
        //! #### col_select
        //! Columns to include in the results. You can use the same mini-language as dplyr::select() to refer to the columns by name. Use c() to use more than one selection expression. Although this usage is less common, col_select also accepts a numeric column index. See ?tidyselect::language for full details on the selection language.
        //!
        //! #### id
        //! The name of a column in which to store the file path. This is useful when reading multiple input files and there is data in the file paths, such as the data collection date. If NULL (the default) no extra column is created.
        //!
        //! #### locale
        //! The locale controls defaults that vary from place to place. The default locale is US-centric (like R), but you can use locale() to create your own locale that controls things like the default time zone, encoding, decimal mark, big mark, and day/month names.
        //!
        //! #### na
        //! Character vector of strings to interpret as missing values. Set this option to character() to indicate no missing values.
        //!
        //! #### comment
        //! A string used to identify comments. Any text after the comment characters will be silently ignored.
        //!
        //! #### trim_ws
        //! Should leading and trailing whitespace (ASCII spaces and tabs) be trimmed from each field before parsing it?
        //!
        //! #### skip
        //! Number of lines to skip before reading data. If comment is supplied any commented lines are ignored after skipping.
        //!
        //! #### n_max
        //! Maximum number of lines to read.
        //!
        //! #### guess_max
        //! Maximum number of lines to use for guessing column types. Will never use more than the number of lines read. See vignette("column-types", package = "readr") for more details.
        //!
        //! #### name_repair
        //! Handling of column names. The default behaviour is to ensure column names are "unique". Various repair strategies are supported:
        //!
        //! "minimal": No name repair or checks, beyond basic existence of names.
        //!
        //! "unique" (default value): Make sure names are unique and not empty.
        //!
        //! "check_unique": no name repair, but check they are unique.
        //!
        //! "universal": Make the names unique and syntactic.
        //!
        //! A function: apply custom name repair (e.g., name_repair = make.names for names in the style of base R).
        //!
        //! A purrr-style anonymous function, see rlang::as_function().
        //!
        //! This argument is passed on as repair to vctrs::vec_as_names(). See there for more details on these terms and the strategies used to enforce them.
        //!
        //! #### num_threads
        //! The number of processing threads to use for initial parsing and lazy reading of data. If your data contains newlines within fields the parser should automatically detect this and fall back to using one thread only. However if you know your file has newlines within quoted fields it is safest to set num_threads = 1 explicitly.
        //!
        //! #### progress
        //! Display a progress bar? By default it will only display in an interactive session and not while knitting a document. The automatic progress bar can be disabled by setting option readr.show_progress to FALSE.
        //!
        //! #### show_col_types
        //! If FALSE, do not show the guessed column types. If TRUE always show the column types, even if they are supplied. If NULL (the default) only show the column types if they are not explicitly supplied by the col_types argument.
        //!
        //! #### skip_empty_rows
        //! Should blank rows be ignored altogether? i.e. If this option is TRUE then blank rows will not be represented at all. If it is FALSE then they will be represented by NA values in all the columns.
        //! # Examples
        //! ```rust
        //! # read local csv
        //! read_csv(readr_example("mtcars.csv"))
        //! # read csv from zip
        //!read_csv(readr_example("mtcars.csv.zip"))
        //! # read csv from bz2
        //!read_csv(readr_example("mtcars.csv.bz2"))
        //! # read csv from url
        //!read_csv("https://github.com/tidyverse/readr/raw/main/inst/extdata/mtcars.csv")
        //! ```
        let url_result = Url::parse(file);
        // make sure the delim is a single character in length
        let delim_bytes: u8 = match delim.len() {
            1 => delim.as_bytes()[0],
            _ => panic!("delim must be a single character"),
        };
        // make a temp dir
        let temp_dir = TempDir::new().unwrap();
        let tmp_file_path = temp_dir.path().join("tmp_ylr.csv");
        let file_path = match url_result {
            Ok(url) => {
                download_data(url.as_str(), tmp_file_path.clone());
                tmp_file_path
            }
            Err(e) => Path::new(file).to_owned(),
        };
        let schema = reader::infer_schema_from_files(
            &[file_path.to_str().unwrap().to_owned()],
            delim_bytes,
            Some(1000),
            true,
        );
        let schema_data_types = reader::infer_schema_from_files(
            &[file_path.to_str().unwrap().to_owned()],
            delim_bytes,
            Some(1000),
            true,
        );
        let file_open = File::open(file_path).unwrap();
        let mut reader = reader::Reader::new(
            file_open,
            Arc::new(schema.expect("Schema should be infered")),
            true,
            Some(delim_bytes),
            1024,
            None,
            None,
            None,
        );

        let record_batch: RecordBatch = reader.next().unwrap().unwrap().clone();
        let tibble = to_pillar(record_batch);
        tibble
    }
}

/// The core ylr database functions
pub mod ylrdb {
    use super::*;
    use arrow2::io::ipc;
    use arrow2::io::ipc::read::FileReader;
    use arrow2::io::ipc::{
        read::read_file_metadata,
        write::{FileWriter, WriteOptions},
    };
    use std::fs::File;
    use std::io::BufReader;
    use std::io::{self, Write};
    use std::sync::Arc;
    use tar::Archive;
    use tempdir::TempDir;

    pub fn ylr_info() {
        // https://fs.r-lib.org/reference/dir_ls.html
    }
    /// new_ylr creates a new ylr database
    pub fn new_ylr(db_name: &str) {
        //! Example
        //! ```rust
        //! use ylr::std::connect_ylr;
        //! let con = connect_ylr("main.ylr");
        //! ```
        println!("Hello, world!");
    }
    pub fn connect_ylr(db_name: &str) {
        //! Example
        //! ```rust
        //! use ylr::std::connect_ylr;
        //! let con = connect_ylr("main.ylr");
        //! ```
        println!("Hello, world!");
    }

    pub fn write_ylr(table: &RecordBatch, ylr_file: &str, table_name: &str) {
        println!("Hello, world!");
    }
    pub fn read_ylr(ylr_file: &str, table_name: &str) {
        // make a tmp dir
        let temp_dir = TempDir::new("unpack").unwrap();
        let tmp_file_path = temp_dir.path().join("tmp.arrow");
        let file = File::open(ylr_file).unwrap();
        let mut ar = Archive::new(file);
        for (i, file) in ar.entries().unwrap().enumerate() {
            let mut file = file.unwrap();
            if file
                .path()
                .expect("Could not unwrap file path")
                .display()
                .to_string()
                == table_name
            {
                //println!("File {}:{}", i, file.path().unwrap().display());
                file.unpack_in("junk")
                    .expect("Could not unpack file in tmp_dir");
            }
        }

        let mut reader = File::open(&tmp_file_path).expect("Could not find file");
        let metadata = read_file_metadata(&mut reader).expect("Could not read metadata");
        let mut reader = FileReader::new(reader, metadata, None, None);
        let row1 = reader.next().unwrap(); // [[-1, 1], [1, -1]]
        let row2 = reader.next().unwrap(); // [[-1, 1], [1, -1]]
        let mut reader = reader.into_inner();
        println!("{:?}", reader);
    }
    pub fn rm_ylr(ylr_file: &str, table_name: &str) {}

    pub fn export_ylr_to_dir(ylr_file: &str, dir: &str) {}
}

#[cfg(test)]
mod tests {
    use super::*;
    use pillar::to_pillar;
    use readr::read_delim;
    use ylrdb::*;

    #[test]
    fn test_read_delim() {
        let a = readr::read_delim(
            "https://raw.githubusercontent.com/mwaskom/seaborn-data/master/penguins.csv",
            ",",
        );
        print!("{}", a);
        let a = readr::read_delim("data/str.csv", ",");
        print!("{}", a)
    }
    #[test]
    fn test_to_tibble() {
        let id_array = Int64Array::from(vec![
            1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24,
            25, 26, 27, 28,
        ]);
        let schema = Schema::new(vec![Field::new("id", DataType::Int64, false)]);
        let record_batch =
            RecordBatch::try_new(Arc::new(schema), vec![Arc::new(id_array)]).unwrap();
        let tibble = to_pillar(record_batch);
        print!("{}", tibble);
    }
    #[test]
    fn test_read_ylr() {
        read_ylr("main.tar", "test.arrow")
    }
}
