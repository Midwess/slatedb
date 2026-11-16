use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

use async_trait::async_trait;
use bytes::Bytes;
use slatedb::{
    config::{DurabilityLevel, ReadOptions, Settings},
    db_cache::{CachedEntry, CachedKey, DbCache},
    object_store::memory::InMemory,
    Db,
};

#[derive(Default)]
struct Cache(Mutex<HashMap<CachedKey, CachedEntry>>);

#[async_trait]
impl DbCache for Cache {
    async fn get_block(&self, key: &CachedKey) -> Result<Option<CachedEntry>, slatedb::Error> {
        Ok(self
            .0
            .lock()
            .expect("test operation succeeds")
            .get(key)
            .cloned())
    }
    async fn get_index(&self, key: &CachedKey) -> Result<Option<CachedEntry>, slatedb::Error> {
        self.get_block(key).await
    }
    async fn get_filter(&self, key: &CachedKey) -> Result<Option<CachedEntry>, slatedb::Error> {
        self.get_block(key).await
    }
    async fn get_stats(&self, key: &CachedKey) -> Result<Option<CachedEntry>, slatedb::Error> {
        self.get_block(key).await
    }
    async fn insert(&self, key: CachedKey, value: CachedEntry) {
        self.0
            .lock()
            .expect("test operation succeeds")
            .insert(key, value);
    }
    async fn remove(&self, key: &CachedKey) {
        self.0.lock().expect("test operation succeeds").remove(key);
    }
    fn entry_count(&self) -> u64 {
        self.0.lock().expect("test operation succeeds").len() as u64
    }
}

fn options() -> ReadOptions {
    ReadOptions {
        durability_filter: DurabilityLevel::Memory,
        ..Default::default()
    }
}

#[tokio::test]
async fn memtable_owns_values_and_observers_do_not_retain_payloads() {
    let db = Db::builder("shared-memtable", Arc::new(InMemory::new()))
        .with_settings(Settings {
            wal_enabled: false,
            ..Default::default()
        })
        .build()
        .await
        .expect("test operation succeeds");
    let payload = Bytes::from(vec![42; 512]);
    let original = payload.as_ptr();
    db.put_bytes(Bytes::from_static(b"k"), payload)
        .await
        .expect("test operation succeeds");
    let value = db
        .get_shared_with_options(b"k", &options())
        .await
        .expect("test operation succeeds")
        .expect("test operation succeeds");
    assert_eq!(value.as_ref().as_ptr(), original);
    let observer = value.downgrade();
    drop(value);
    assert_eq!(
        observer
            .upgrade()
            .expect("test operation succeeds")
            .as_ref()
            .as_ptr(),
        original
    );
    let guard = observer.upgrade().expect("test operation succeeds");
    db.put(b"k", b"updated")
        .await
        .expect("test operation succeeds");
    assert_eq!(
        db.get_shared_with_options(b"k", &options())
            .await
            .expect("test operation succeeds")
            .expect("test operation succeeds")
            .as_ref(),
        b"updated"
    );
    db.delete(b"k").await.expect("test operation succeeds");
    assert!(db
        .get_shared_with_options(b"k", &options())
        .await
        .expect("test operation succeeds")
        .is_none());
    db.close().await.expect("test operation succeeds");
    drop(db);
    assert_eq!(guard.as_ref(), &[42; 512]);
    drop(guard);
    assert!(observer.upgrade().is_none());
}

#[tokio::test]
async fn first_load_and_cache_hits_share_one_block_until_eviction() {
    let store = Arc::new(InMemory::new());
    let cache = Arc::new(Cache::default());
    let settings = Settings {
        wal_enabled: false,
        ..Default::default()
    };
    let db = Db::builder("shared-block", store.clone())
        .with_settings(settings.clone())
        .with_db_cache(cache.clone(), 1)
        .build()
        .await
        .expect("test operation succeeds");
    db.put(b"a", b"first")
        .await
        .expect("test operation succeeds");
    db.put(b"b", b"second")
        .await
        .expect("test operation succeeds");
    db.flush().await.expect("test operation succeeds");
    db.close().await.expect("test operation succeeds");
    drop(db);
    cache.0.lock().expect("test operation succeeds").clear();
    let db = Db::builder("shared-block", store)
        .with_settings(settings)
        .with_db_cache(cache.clone(), 1)
        .build()
        .await
        .expect("test operation succeeds");
    let first = db
        .get_shared_with_options(b"a", &options())
        .await
        .expect("test operation succeeds")
        .expect("test operation succeeds");
    let pointer = first.as_ref().as_ptr();
    let observer = first.downgrade();
    drop(first);
    assert!(observer.upgrade().is_some());
    let second = db
        .get_shared_with_options(b"a", &options())
        .await
        .expect("test operation succeeds")
        .expect("test operation succeeds");
    assert_eq!(second.as_ref().as_ptr(), pointer);
    let ordinary = db
        .get_with_options(b"a", &options())
        .await
        .expect("test operation succeeds")
        .expect("test operation succeeds");
    assert_eq!(ordinary.as_ptr(), pointer);
    drop(ordinary);
    let adjacent = db
        .get_shared_with_options(b"b", &options())
        .await
        .expect("test operation succeeds")
        .expect("test operation succeeds");
    assert_eq!(adjacent.as_ref(), b"second");
    assert!(adjacent.allocation_len() > adjacent.as_ref().len());
    let adjacent_observer = adjacent.downgrade();
    drop(adjacent);
    cache.0.lock().expect("test operation succeeds").clear();
    assert_eq!(second.as_ref(), b"first");
    assert!(adjacent_observer.upgrade().is_some());
    drop(second);
    assert!(observer.upgrade().is_none());
    assert!(adjacent_observer.upgrade().is_none());
    assert_eq!(
        db.get_shared_with_options(b"a", &options())
            .await
            .expect("test operation succeeds")
            .expect("test operation succeeds")
            .as_ref(),
        b"first"
    );
    db.close().await.expect("test operation succeeds");
}
