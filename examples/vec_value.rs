use lsm_engine::LSMBuilder;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut lsm = LSMBuilder::new().
       segment_size(2000). // each sst file will have up to 2000 entries
       inmemory_capacity(100). //store only 100 entries in memory
       sparse_offset(20). //store one out of every 20 entries written into segments in memory
       wal_path("/tmp/vec_value_rs_wal.ndjson"). //path
       build();

    let dataset = vec![
        ("k1", vec![1, 2, 3]),
        ("k2", vec![4, 5, 6]),
        ("k1", vec![7, 8, 9]),
    ];

    for (k, v) in dataset.iter() {
        lsm.write(String::from(*k), serde_json::to_string(v)?)?;
    }

    let k1: Vec<i32> = serde_json::from_str(lsm.read("k1")?.unwrap().as_str())?;
    dbg!(&k1);

    Ok(())
}
