use lsm_engine::{LSMBuilder, LSMEngine};

use std::collections::HashSet;

const DOC_MAX_ID: &str = "doc_id_max";

type DocId = usize;

struct InvertedIndex {
    index: LSMEngine,
    doc_store: LSMEngine,
    doc_id_max: LSMEngine,
}

impl InvertedIndex {
    fn new() -> Self {
        Self {
            index: LSMBuilder::new().
                segment_size(2000). // each sst file will have up to 2000 entries
                inmemory_capacity(100). //store only 100 entries in memory
                sparse_offset(20). //store one out of every 20 entries written into segments in memory
                wal_path("/tmp/index_wal.ndjson"). //path
                build(),
            doc_store: LSMBuilder::new().
                    segment_size(2000). // each sst file will have up to 2000 entries
                    inmemory_capacity(100). //store only 100 entries in memory
                    sparse_offset(20). //store one out of every 20 entries written into segments in memory
                    wal_path("/tmp/doc_store_wal.ndjson"). //path
                    build(),
            doc_id_max: LSMBuilder::new().
                        segment_size(2000). // each sst file will have up to 2000 entries
                        inmemory_capacity(100). //store only 100 entries in memory
                        sparse_offset(20). //store one out of every 20 entries written into segments in memory
                        wal_path("/tmp/doc_id_max_wal.ndjson"). //path
                        build(),
        }
    }

    fn get_document(&mut self, doc_id: DocId) -> String {
        self.doc_store
            .read(&doc_id.to_string().as_str())
            .unwrap()
            .unwrap()
    }

    fn add_document(&mut self, document: &str) -> Result<(), Box<dyn std::error::Error>> {
        let doc_id = self
            .doc_id_max
            .read(DOC_MAX_ID)?
            .unwrap_or("0".to_string())
            .parse::<usize>()?;
        self.doc_store
            .write(doc_id.to_string(), document.to_string())?;

        let mut tokens = document.split_whitespace();
        while let Some(token) = tokens.next() {
            let mut plist = self
                .index
                .read(&token.to_string())?
                .unwrap_or("[]".to_string());
            // we don't deserialize back and forth, just mutate the string directly
            let closer = plist.pop().unwrap();
            assert!(closer == ']');
            if plist.len() > 1 {
                plist.push_str(format!(",{}]", doc_id).as_str());
            } else {
                plist.push_str(format!("{}]", doc_id).as_str());
            }
            self.index.write(token.to_string(), plist)?;
        }
        self.doc_id_max
            .write(DOC_MAX_ID.to_string(), (doc_id + 1).to_string())?;
        Ok(())
    }

    fn update_document(
        &mut self,
        doc_id: DocId,
        new_content: &str,
    ) -> Result<(), Box<dyn std::error::Error>> {
        // why don't we simply remove then add? because we want to preserve doc_id
        // we need a copy of the original content so we can find the plists
        let previous_content = self.get_document(doc_id).clone();
        self.doc_store
            .write(doc_id.to_string(), new_content.to_string())?;
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
                // we gotta deserialize to modify
                let mut plist: Vec<usize> = serde_json::from_str(
                    &self.index.read(p.to_string().as_str()).unwrap().unwrap(),
                )?;
                // we don't guarantee order of plist to make delete faster here
                plist.swap_remove(
                    plist
                        .iter()
                        .position(|x| *x == doc_id)
                        .expect("doc_id not found"),
                );
                // then reserialize to store
                self.index
                    .write(p.to_string(), serde_json::to_string(&plist)?)?;
            } else {
                // for "real" inverted index the logic is a bit more complex,
                // as in bm25, eg., doc len changes also change the score every term has
                // but for our simple setup, we saw this term before, so it isn't new
                nhash.remove(p);
            }
        }
        // what's left is brand new
        for n in nhash.iter() {
            let mut plist = self
                .index
                .read(&n.to_string())
                .unwrap_or(Some("[]".to_string()))
                .unwrap();
            // we don't deserialize back and forth, just mutate the string directly
            let closer = plist.pop().unwrap();
            assert!(closer == ']');
            if plist.len() > 1 {
                plist.push_str(format!(",{}]", doc_id).as_str());
            } else {
                plist.push_str(format!("{}]", doc_id).as_str());
            }
            self.index.write(n.to_string(), plist)?;
        }
        Ok(())
    }

    fn delete_document(&mut self, doc_id: DocId) -> Result<(), Box<dyn std::error::Error>> {
        let document = self.get_document(doc_id).clone();
        self.doc_store.delete(&doc_id.to_string().as_str())?;
        // then we remove it from the index having identified its posting lists
        let mut tokens = document.split_whitespace();
        while let Some(token) = tokens.next() {
            // we gotta deserialize to modify
            let mut plist: Vec<usize> = serde_json::from_str(
                &self
                    .index
                    .read(token.to_string().as_str())
                    .unwrap()
                    .unwrap(),
            )?;
            // we don't guarantee order of plist to make delete faster here
            plist.swap_remove(
                plist
                    .iter()
                    .position(|x| *x == doc_id)
                    .expect("doc_id not found"),
            );
            // then reserialize to store
            self.index
                .write(doc_id.to_string(), serde_json::to_string(&plist)?)?;
        }
        Ok(())
    }

    // searching can mut because of the underlying LSM read mut
    fn search(&mut self, query: &str) -> Vec<DocId> {
        let mut results = vec![];
        let mut tokens = query.split_whitespace();

        while let Some(token) = tokens.next() {
            if let Some(doc_ids) = self.index.read(token).expect("found token in index") {
                let doc_ids: Vec<usize> =
                    serde_json::from_str(&doc_ids.clone()).expect("valid plist");
                results.extend(doc_ids);
            }
        }

        results
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let docs = vec![
        "some document i added",
        "some other document i added",
        "no tokens in common",
    ];
    let mut ii = InvertedIndex::new();
    for doc in &docs {
        ii.add_document(doc)?;
    }
    let q = "added";
    println!(
        "We're searching for docs with '{}' in them, out of docs={:?}",
        q, &docs
    );
    let results = ii.search(q);
    println!("With all documents indexed, expect 2, found: {:?}", results);
    ii.delete_document(1)?;
    let results = ii.search("added");
    println!(
        "With one removed (now docs={:?}), expect 2, found: {:?}",
        &docs, results
    );
    ii.update_document(2, "tokens in common added")?;
    let results = ii.search("added");
    println!(
        "With one removed and one updated (now docs={:?}), expect 3, found: {:?}",
        &docs, results
    );

    Ok(())
}
