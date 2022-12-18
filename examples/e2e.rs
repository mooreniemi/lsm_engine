use lsm_engine::LSMBuilder;
use std::fs::File;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut lsm = LSMBuilder::new().
       segment_size(2000). // each sst file will have up to 2000 entries
       inmemory_capacity(100). //store only 100 entries in memory
       sparse_offset(20). //store one out of every 20 entries written into segments in memory
       wal_path("/tmp/e2e_wal.ndjson"). //path
       build();

    let mut default_lsm = LSMBuilder::new().build(); //an lsm engine with default parameters

    let dataset = vec![("k1", "v1"), ("k2", "v2"), ("k1", "v_1_1")];

    for (k, v) in dataset.iter() {
        lsm.write(String::from(*k), String::from(*v))?;
    }
    assert_eq!(lsm.read("k1")?, Some("v_1_1".to_owned()));

    let wal = File::open("my_write_ahead_log.txt")?;
    default_lsm.recover_from(wal)?;
    for (k, _v) in dataset {
        assert!(default_lsm.contains(k)?);
    }

    std::fs::remove_file("/tmp/e2e_wal.ndjson")?;

    Ok(())
}
