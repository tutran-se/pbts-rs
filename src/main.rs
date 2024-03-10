#![allow(dead_code)]

use std::{
    fs::File,
    io::{BufReader, BufWriter, Read, Write},
    time::Instant,
};

use flate2::bufread::GzDecoder;
use rayon::prelude::*;
use serde::{Deserialize, Serialize};

fn write_all(file_path: &str) {
    let mut file = File::create(file_path).unwrap();
    file.write_all(b"hello world")
        .expect("Write all buffer to file");
}
fn read_to_string(file_path: &str) -> String {
    let mut file = File::open(file_path).unwrap();
    let mut contents = String::new();
    if let Err(err) = file.read_to_string(&mut contents) {
        println!("{:?}", err);
    }
    contents
}

fn buffer_read_to_string(file_path: &str) -> String {
    let file = File::open(file_path).unwrap();
    let mut buff_reader = BufReader::with_capacity(1000000, file);
    let mut contents = String::new();
    if let Err(e) = buff_reader.read_to_string(&mut contents) {
        println!("{:?}", e);
    }

    contents
}

#[derive(Serialize, Deserialize, Debug)]
struct Item {
    ts: Vec<i64>,
    v: Vec<i64>,
}

// fn main() {
//     let file_path = "data.json";

//     let file = File::open(file_path).unwrap();
//     let buff_reader = BufReader::with_capacity(1000000, file);

//     let start = Instant::now();
//     let items: Vec<DataPoint> = serde_json::from_reader(buff_reader).unwrap();

//     let duration = start.elapsed();

//     println!("{:?}", duration);
//     println!("{:?}", items[0]);
// }
// fn main() {
//     let file_path = "data3.json";

//     let start = Instant::now();
//     // file reader
//     let contents = buffer_read_to_string(file_path);
//     let duration = start.elapsed();

//     println!("Reading time: {:?}", duration);

//     let start = Instant::now();
//     let my_item: Item = serde_json::from_str(&contents).unwrap();

//     let duration = start.elapsed();

//     println!("Deserilization time: {:?}", duration);

//     println!("Ts: {:?}", my_item.ts[0]);
//     println!("V: {:?}", my_item.v[0]);
// }

// fn main2() {
//     let file_path = "data.json";

//     // file reader
//     let contents = buffer_read_to_string(file_path);

//     let start = Instant::now();
//     let items: Vec<DataPoint> = serde_json::from_str(&contents).unwrap();

//     let mut ts: Vec<i64> = vec![];
//     let mut v: Vec<f64> = vec![];

//     for item in &items {
//         ts.push(item.ts);
//         v.push(item.v);
//     }
//     let my_item: Item = Item { ts, v };

//     let new_file_path = "data2.json";
//     let file = File::create(new_file_path).unwrap();

//     let buff_writer = BufWriter::new(file);

//     serde_json::to_writer(buff_writer, &my_item).unwrap();

//     let duration = start.elapsed();

//     println!("{:?}", duration);
// }

// fn main() {
//     let file_path = "data.json";

//     let start = Instant::now();
//     // file reader
//     let contents = buffer_read_to_string(file_path);
//     let duration = start.elapsed();

//     println!("Reading time: {:?}", duration);

//     let start = Instant::now();

//     let items: Vec<DataPoint> = serde_json::from_str(&contents).unwrap_or_else(|err| {
//         eprintln!("ERROR: Failed to parse to json");
//         Vec::new()
//     });
//     let duration = start.elapsed();

//     println!("Deserilization time: {:?}", duration);
// }

// Deserilizatio using buffer reader
// fn main() {
//     let file_path = "data3.json";

//     let file = File::open(file_path).unwrap();
//     let reader = BufReader::new(file);
//     // let start = Instant::now();
//     let my_item: Item = serde_json::from_reader(reader).unwrap();
//     // let duration = start.elapsed();

//     // println!("{:?}", duration);

//     // println!("Ts: {:?}", my_item.ts[0]);
//     // println!("V: {:?}", my_item.v[0]);
// }

// Deserilizatio data.bin file
// fn main() {
//     let file_path = "data2.bin";

//     let start = Instant::now();
//     let file = File::open(file_path).unwrap();
//     let mut buff_reader = BufReader::new(file);
//     let mut buffer: Vec<u8> = vec![];
//     buff_reader.read_to_end(&mut buffer);

//     let my_item: Item = bincode::deserialize(&buffer).unwrap();
//     let duration = start.elapsed();

//     println!("{:?}", duration);

//     println!("Ts: {:?}", my_item.ts.len());
//     println!("V: {:?}", my_item.v.len());
// }

// Deserialize gz file
// fn main() {
//     // READ FILE
//     let file_path = "data.gz";

//     let start = Instant::now();
//     let file = File::open(file_path).unwrap();
//     let mut buff_reader = BufReader::new(file);
//     let mut buffer: Vec<u8> = vec![];
//     buff_reader.read_to_end(&mut buffer).unwrap();

//     // DECOMPRESS
//     let mut decoder = GzDecoder::new(&buffer[..]);
//     let mut decompressed = Vec::new();
//     decoder.read_to_end(&mut decompressed).unwrap();

//     let my_item: Item = bincode::deserialize(&decompressed).unwrap();
//     let duration = start.elapsed();

//     println!("{:?}", duration);

//     println!("Ts: {:?}", my_item.ts.len());
//     println!("V: {:?}", my_item.v.len());
// }

// RLE

use std::thread;

#[derive(Debug, Serialize, Deserialize)]
struct RLEData {
    ts: Vec<(i64, i32)>,
    v: Vec<(i64, i32)>,
}

#[derive(Debug)]
struct DataPoint {
    ts: i64,
    v: f64,
    intervalGroup: i64,
}

#[derive(Debug)]
struct Sum {
    totalSum: f64,
    intervalGroup: i64,
}

fn main() {
    let tuple = fetch_data();

    // let starter = Instant::now();
    // sum_by_group_normal(&tuple, 1707152400, 86400);
    // let duration = starter.elapsed();
    // println!("Normal {:?}", duration);

    let starter = Instant::now();
    sum_aggregation(&tuple, 1707152400, 60);
    let duration = starter.elapsed();
    println!("Aggregation (1 param): {:?}", duration);
}

fn fetch_data() -> (Vec<i64>, Vec<i64>) {
    let file_path = "data2.bin";

    let file = File::open(file_path).unwrap();
    let mut buff_reader = BufReader::new(file);
    let mut buffer: Vec<u8> = vec![];
    buff_reader.read_to_end(&mut buffer);

    let my_item: RLEData = bincode::deserialize(&buffer).unwrap();

    // Revert RLE more efficiently
    let mut original_data_ts: Vec<i64> = Vec::new();
    for &(value, count) in my_item.ts.iter() {
        original_data_ts.extend(std::iter::repeat(value).take(count as usize));
    }

    let mut original_data_v: Vec<i64> = Vec::new();
    for &(value, count) in my_item.v.iter() {
        original_data_v.extend(std::iter::repeat(value).take(count as usize));
    }
    // Revert DE for ts
    let mut rever_de_original_data_ts: Vec<i64> = Vec::with_capacity(original_data_ts.len());

    if !original_data_ts.is_empty() {
        // The first value is stored directly, not as a delta.
        let mut current_value = original_data_ts[0];
        rever_de_original_data_ts.push(current_value);

        // Iterate over the rest of the values, reconstructing the original data.
        for &delta in original_data_ts.iter().skip(1) {
            current_value += delta;
            rever_de_original_data_ts.push(current_value);
        }
    }

    // Revert DE for value
    let mut rever_de_original_data_v: Vec<i64> = Vec::with_capacity(original_data_v.len());

    if !original_data_v.is_empty() {
        // The first value is stored directly, not as a delta.
        let mut current_value = original_data_v[0];
        rever_de_original_data_v.push(current_value);

        // Iterate over the rest of the values, reconstructing the original data.
        for &delta in original_data_v.iter().skip(1) {
            current_value += delta;
            rever_de_original_data_v.push(current_value);
        }
    }

    (rever_de_original_data_ts, rever_de_original_data_v)
}

fn sum_aggregation(tuple: &(Vec<i64>, Vec<i64>), start_ts: i64, interval: i64) -> Vec<Sum> {
    let (ts_items, v_items) = tuple;

    let data_items: Vec<DataPoint> = ts_items
        .par_iter()
        .enumerate()
        .map(|(i, ts)| DataPoint {
            ts: *ts,
            v: (v_items[i] / 1000) as f64,
            intervalGroup: (ts - start_ts) / interval,
        })
        .collect();

    // The rest of the function remains sequential since it involves state-dependent processing
    let mut sums: Vec<Sum> = vec![];
    let mut sum_by_group: f64 = 0.0;
    let mut current_interval_group = data_items[0].intervalGroup;

    for item in data_items {
        let DataPoint {
            v, intervalGroup, ..
        } = item;

        if intervalGroup == current_interval_group {
            sum_by_group += v;
        } else {
            sums.push(Sum {
                totalSum: sum_by_group,
                intervalGroup: current_interval_group,
            });
            current_interval_group = intervalGroup;
            sum_by_group = v;
        }
    }

    sums
}
