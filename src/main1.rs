use flate2::write::GzEncoder;
use flate2::Compression;
use serde::{Deserialize, Serialize};
use std::{
    fs::File,
    io::{BufReader, BufWriter, Read, Write},
};

#[derive(Debug, Serialize, Deserialize)]
struct TsValueList {
    ts: Vec<i64>,
    v: Vec<f64>,
}

#[derive(Debug, Serialize, Deserialize)]
struct NewTsValueList {
    ts: Vec<i64>,
    v: Vec<i64>,
}
// fn main() {
//     //read data2.json file
//     let file_path = "data2.json";
//     let mut file = File::open(file_path).unwrap();

//     let mut result = String::new();
//     file.read_to_string(&mut result).unwrap();

//     // convert json string to obj {ts,v}
//     let tsv_list: TsValueList = serde_json::from_str(&result).unwrap();

//     println!("Ts: {:?}", tsv_list.ts[0]);
//     println!("V: {:?}", tsv_list.v[0]);

//     let new_tsv_list = NewTsValueList {
//         ts: tsv_list.ts,
//         v: tsv_list.v.iter().map(|&v| (v * 1000.0) as i64).collect(),
//     };

//     println!("Ts: {:?}", new_tsv_list.ts[0]);
//     println!("V: {:?}", new_tsv_list.v[0]);

//     // save to data3.json
//     let file_path = "data3.json";
//     let file = File::create(file_path).unwrap();
//     let buf_writer = BufWriter::new(file);

//     serde_json::to_writer(buf_writer, &new_tsv_list).unwrap_or_else(|e| {
//         eprintln!("ERROR: Fail to save json file {file_path}");
//         ()
//     });
// }

// fn main() {
//     //read data2.json file
//     let file_path = "data2.json";
//     let mut file = File::open(file_path).unwrap();

//     let mut result = String::new();
//     file.read_to_string(&mut result).unwrap();

//     // convert json string to obj {ts,v}
//     let tsv_list: TsValueList = serde_json::from_str(&result).unwrap();

//     println!("Ts: {:?}", tsv_list.ts[0]);
//     println!("V: {:?}", tsv_list.v[0]);

//     let new_tsv_list = NewTsValueList {
//         ts: tsv_list.ts,
//         v: tsv_list.v.iter().map(|&v| (v * 1000.0) as i64).collect(),
//     };

//     println!("Ts: {:?}", new_tsv_list.ts[0]);
//     println!("V: {:?}", new_tsv_list.v[0]);

//     // save to data3.json
//     let serialized = bincode::serialize(&new_tsv_list).unwrap();

//     let file_path = "data.bin";
//     let file = File::create(file_path).unwrap();
//     let mut buf_writer = BufWriter::new(file);
//     buf_writer.write_all(&serialized);
// }

// // Compress serielized biar data
// fn main() {
//     //read data2.json file
//     let file_path = "data2.json";
//     let mut file = File::open(file_path).unwrap();

//     let mut result = String::new();
//     file.read_to_string(&mut result).unwrap();

//     // convert json string to obj {ts,v}
//     let tsv_list: TsValueList = serde_json::from_str(&result).unwrap();

//     println!("Ts: {:?}", tsv_list.ts[0]);
//     println!("V: {:?}", tsv_list.v[0]);

//     let new_tsv_list = NewTsValueList {
//         ts: tsv_list.ts,
//         v: tsv_list.v.iter().map(|&v| (v * 1000.0) as i64).collect(),
//     };

//     println!("Ts: {:?}", new_tsv_list.ts[0]);
//     println!("V: {:?}", new_tsv_list.v[0]);

//     // save to data3.json
//     let serialized = bincode::serialize(&new_tsv_list).unwrap();

//     let mut encoder = GzEncoder::new(Vec::new(), Compression::default());

//     encoder.write_all(&serialized).unwrap();

//     let compressed = encoder.finish().unwrap();

//     let file_path = "data.gz";
//     let file = File::create(file_path).unwrap();
//     let mut buf_writer = BufWriter::new(file);
//     buf_writer.write_all(&compressed).unwrap();
// }

#[derive(Debug, Serialize, Deserialize)]
struct RLEData {
    ts: Vec<(i64, i32)>,
    v: Vec<(i64, i32)>,
}
fn main() {
    //read data2.json file
    let file_path = "data2.json";
    let mut file = File::open(file_path).unwrap();

    let mut result = String::new();
    file.read_to_string(&mut result).unwrap();

    // convert json string to obj {ts,v}
    let tsv_list: TsValueList = serde_json::from_str(&result).unwrap();

    let new_tsv_list = NewTsValueList {
        ts: tsv_list.ts,
        // v: tsv_list.v.iter().map(|&v| (v * 1000.0) as i64).collect(),
        v: tsv_list.v.iter().map(|&v| 1000 as i64).collect(),
    };

    let ts = new_tsv_list.ts;
    let v: Vec<i64> = new_tsv_list.v;

    println!("{:?}", &ts[ts.len() - 10..]);
    println!("{:?}", &v[v.len() - 10..]);

    let mut new_ts: Vec<i64> = Vec::new();
    let mut new_v: Vec<i64> = Vec::new();

    // Delta Encoding
    let mut last_item: i64 = 0;
    for (idx, item) in ts.iter().enumerate() {
        if idx == 0 {
            new_ts.push(*item);
        } else {
            new_ts.push(item - last_item);
        }
        last_item = *item;
    }

    for (idx, &item) in v.iter().enumerate() {
        if idx == 0 {
            new_v.push(*item);
        } else {
            new_v.push(item - last_item);
        }
        last_item = *item;
    }

    // println!("{:?}", &new_ts[new_ts.len() - 10..]);
    // println!("{:?}", &new_v[new_v.len() - 10..]);

    // RLE
    let mut rle_items_ts: Vec<(i64, i32)> = Vec::new();

    let mut counter = 0;
    let mut current_item: i64 = new_ts[0];

    for item in new_ts.iter() {
        if item == &current_item {
            counter += 1
        } else {
            rle_items_ts.push((current_item, counter));
            current_item = *item;
            counter = 1;
        }
    }
    rle_items_ts.push((current_item, counter));

    println!("{:?}", rle_items_ts);

    let mut rle_items_v: Vec<(i64, i32)> = Vec::new();

    let mut counter = 0;
    let mut current_item: i64 = new_v[0];

    for item in new_v.iter() {
        if item == &current_item {
            counter += 1
        } else {
            rle_items_v.push((current_item, counter));
            current_item = *item;
            counter = 1;
        }
    }
    rle_items_v.push((current_item, counter));

    println!("{:?}", &rle_items_v[0..2]);

    let rle_data = RLEData {
        ts: rle_items_ts,
        v: rle_items_v,
    };
    // println!("{:?}", &new_v[0..10]);

    // let new_tsv_list: NewTsValueList = NewTsValueList {
    //     ts: new_ts,
    //     v: new_v,
    // };

    // println!("{:?}", new_tsv_list.ts[1]);
    // println!("{:?}", new_tsv_list.v[1]);

    // // save to data2.bin
    let serialized = bincode::serialize(&rle_data).unwrap();

    let file_path = "data2.bin";
    let file = File::create(file_path).unwrap();
    let mut buf_writer = BufWriter::new(file);
    buf_writer.write_all(&serialized);
}
