use lsm_engine::LSMBuilder;

use std::collections::{HashMap, HashSet};

#[derive(Debug)]
struct InvertedIndex {
    index: HashMap<String, Vec<usize>>,
    doc_store: Vec<String>,
}

impl InvertedIndex {
    fn new() -> Self {
        Self {
            index: HashMap::new(),
            doc_store: Vec::new(),
        }
    }

    fn get_document(&mut self, doc_id: usize) -> String {
        self.doc_store.get(doc_id).unwrap().to_string()
    }

    fn add_document(&mut self, document: &str) {
        self.doc_store.push(document.to_string());
        let doc_id = self.doc_store.len() - 1;

        let mut tokens = document.split_whitespace();
        while let Some(token) = tokens.next() {
            let entry = self.index.entry(token.to_string()).or_insert(vec![]);
            entry.push(doc_id);
        }
    }

    fn update_document(
        &mut self,
        doc_id: usize,
        new_content: &str,
    ) -> Result<(), Box<dyn std::error::Error>> {
        // why don't we simply remove then add? because we want to preserve doc_id
        // we need a copy of the original content so we can find the plists
        let previous_content = self.get_document(doc_id).clone();
        self.doc_store[doc_id] = new_content.to_string();
        // we just remove it from the index
        let mut previous_tokens = previous_content.split_whitespace();
        let mut new_tokens = new_content.split_whitespace();
        // tokens in common mean no changes
        let mut phash = HashSet::new();
        while let Some(token) = previous_tokens.next() {
            phash.insert(token);
        }
        let mut nhash = HashSet::new();
        while let Some(token) = new_tokens.next() {
            nhash.insert(token);
        }
        for p in phash.iter() {
            // the update removes this term
            if !nhash.contains(p) {
                let plist = self.index.get_mut(&p.to_string()).unwrap();
                // we don't guarantee order of plist to make delete faster here
                plist.swap_remove(
                    plist
                        .iter()
                        .position(|x| *x == doc_id)
                        .expect("doc_id not found"),
                );
            } else {
                // for "real" inverted index the logic is a bit more complex,
                // as in bm25, eg., doc len changes also change the score every term has
                // but for our simple setup, we saw this term before, so it isn't new
                nhash.remove(p);
            }
        }
        // what's left is brand new
        for n in nhash.iter() {
            let plist = self.index.get_mut(&n.to_string()).unwrap();
            plist.push(doc_id);
        }
        Ok(())
    }

    fn delete_document(&mut self, doc_id: usize) -> Result<(), Box<dyn std::error::Error>> {
        let document = self.get_document(doc_id).clone();
        // we don't remove it because the other ids would shift and disrupt O(1) gets
        // self.doc_store.remove(doc_id);
        // instead we just introduce empty content at the id
        self.doc_store[doc_id] = "".to_string();
        // then we remove it from the index having identified its posting lists
        let mut tokens = document.split_whitespace();
        while let Some(token) = tokens.next() {
            let plist = self.index.get_mut(token).unwrap();
            plist.swap_remove(
                plist
                    .iter()
                    .position(|x| *x == doc_id)
                    .expect("doc_id not found"),
            );
        }
        Ok(())
    }

    fn search(&self, query: &str) -> Vec<usize> {
        let mut results = vec![];
        let mut tokens = query.split_whitespace();

        while let Some(token) = tokens.next() {
            if let Some(doc_ids) = self.index.get(token) {
                results.extend(doc_ids);
            }
        }

        results
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut ii = InvertedIndex::new();
    ii.add_document("some document i added");
    ii.add_document("some other document i added");
    ii.add_document("no tokens in common");
    dbg!(&ii);
    let results = ii.search("added");
    dbg!(&results);
    ii.delete_document(1)?;
    let results = ii.search("added");
    dbg!(&results);
    ii.update_document(2, "tokens in common added")?;
    let results = ii.search("added");
    dbg!(&results);

    let mut lsm = LSMBuilder::new().
        segment_size(2000). // each sst file will have up to 2000 entries
        inmemory_capacity(100). //store only 100 entries in memory
        sparse_offset(20). //store one out of every 20 entries written into segments in memory
        wal_path("/tmp/inverted_index_rs_wal.ndjson"). //path
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
