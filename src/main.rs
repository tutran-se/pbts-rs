#![allow(dead_code)]

use std::{
    fs::File,
    io::{BufReader, BufWriter, Read, Write},
    time::Instant,
};

use flate2::bufread::GzDecoder;
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
struct DataPoint {
    ts: i64,
    v: f64,
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

#[derive(Debug, Serialize, Deserialize)]
struct RLEData {
    ts: Vec<(i64, i32)>,
    v: Vec<(i64, i32)>,
}
fn main() {
    let file_path = "data2.bin";

    let start = Instant::now();
    let file = File::open(file_path).unwrap();
    let mut buff_reader = BufReader::new(file);
    let mut buffer: Vec<u8> = vec![];
    buff_reader.read_to_end(&mut buffer);

    let my_item: RLEData = bincode::deserialize(&buffer).unwrap();

    // Revert RLE
    // let mut original_data_ts: Vec<i64> = Vec::new();

    // for &(value, count) in my_item.ts.iter() {
    //     for _ in 0..count {
    //         original_data_ts.push(value);
    //     }
    // }

    // let mut original_data_v: Vec<i64> = Vec::new();

    // for &(value, count) in my_item.v.iter() {
    //     for _ in 0..count {
    //         original_data_v.push(value);
    //     }
    // }
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

    let total_v: i64 = rever_de_original_data_v.iter().sum();

    let duration = start.elapsed();

    println!("{:?}", duration);
    println!("Total V: {:?}", total_v);

    println!("Ts: {:?}", &rever_de_original_data_ts[0..10]);
    println!("Ts: {:?}", &rever_de_original_data_v[0..10]);
    println!("Ts: {:?}", rever_de_original_data_ts.len());
    println!("Ts: {:?}", rever_de_original_data_v.len());
}

// fn main() {
//     let file_path = "data2.bin";
//     let start = Instant::now();
//     let file = File::open(file_path).unwrap();
//     let mut buff_reader = BufReader::new(file);
//     let mut buffer: Vec<u8> = vec![];
//     buff_reader.read_to_end(&mut buffer).unwrap();

//     let my_item: RLEData = bincode::deserialize(&buffer).unwrap();

//     // Revert RLE more efficiently
//     let mut original_data_ts: Vec<i64> = Vec::new();
//     for &(value, count) in my_item.ts.iter() {
//         original_data_ts.extend(std::iter::repeat(value).take(count as usize));
//     }

//     let mut original_data_v: Vec<i64> = Vec::new();
//     for &(value, count) in my_item.v.iter() {
//         original_data_v.extend(std::iter::repeat(value).take(count as usize));
//     }

//     // Revert Delta Encoding (DE) more efficiently
//     let mut rever_de_original_data_ts = if !original_data_ts.is_empty() {
//         original_data_ts.iter()
//                         .scan(original_data_ts[0], |state, &x| { *state += x; Some(*state) })
//                         .collect::<Vec<i64>>()
//     } else {
//         Vec::new()
//     };

//     let mut rever_de_original_data_v = if !original_data_v.is_empty() {
//         original_data_v.iter()
//                        .scan(original_data_v[0], |state, &x| { *state += x; Some(*state) })
//                        .collect::<Vec<i64>>()
//     } else {
//         Vec::new()
//     };

//     let duration = start.elapsed();

//     println!("{:?}", duration);
//     println!("Ts: {:?}", &rever_de_original_data_ts[0..10.min(rever_de_original_data_ts.len())]);
//     println!("V: {:?}", &rever_de_original_data_v[0..10.min(rever_de_original_data_v.len())]);
//     println!("Ts length: {:?}", rever_de_original_data_ts.len());
//     println!("V length: {:?}", rever_de_original_data_v.len());
// }
