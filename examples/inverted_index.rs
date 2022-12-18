use lsm_engine::{LSMBuilder, LSMEngine};

use std::collections::{HashMap, HashSet};

const DOC_MAX_ID: &str = "doc_id_max";

type DocId = usize;
type PostingList = Vec<DocId>;
type Term = String;

struct InvertedIndex {
    index: HashMap<Term, PostingList>,
    doc_store: LSMEngine,
    doc_id_max: LSMEngine,
}

impl InvertedIndex {
    fn new() -> Self {
        Self {
            index: HashMap::new(),
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
            let entry = self.index.entry(token.to_string()).or_insert(vec![]);
            entry.push(doc_id);
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

    fn delete_document(&mut self, doc_id: DocId) -> Result<(), Box<dyn std::error::Error>> {
        let document = self.get_document(doc_id).clone();
        self.doc_store.delete(&doc_id.to_string().as_str())?;
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

    fn search(&self, query: &str) -> Vec<DocId> {
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
    ii.add_document("some document i added")?;
    ii.add_document("some other document i added")?;
    ii.add_document("no tokens in common")?;
    let results = ii.search("added");
    dbg!(&results);
    ii.delete_document(1)?;
    let results = ii.search("added");
    dbg!(&results);
    ii.update_document(2, "tokens in common added")?;
    let results = ii.search("added");
    dbg!(&results);

    Ok(())
}
