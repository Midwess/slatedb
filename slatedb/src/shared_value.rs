use std::ops::Range;
use std::sync::{Arc, Weak};

use bytes::Bytes;

/// A value range that shares its immutable allocation with a memtable or SST block.
/// Holding this handle keeps the allocation alive. No value bytes are copied.
#[derive(Clone, Debug)]
pub struct SharedValue {
    owner: Arc<Bytes>,
    range: Range<usize>,
}

/// A weak observer of one immutable value version.
/// An observer does not retain the payload. It does not check freshness or expiration.
#[derive(Clone, Debug)]
pub struct WeakValue {
    owner: Weak<Bytes>,
    range: Range<usize>,
}

impl SharedValue {
    pub(crate) fn new(bytes: Bytes) -> Self {
        let range = 0..bytes.len();
        Self {
            owner: Arc::new(bytes),
            range,
        }
    }

    pub(crate) fn from_owner(owner: Arc<Bytes>, value: &Bytes) -> Option<Self> {
        let start = (value.as_ptr() as usize).checked_sub(owner.as_ptr() as usize)?;
        let end = start.checked_add(value.len())?;
        (end <= owner.len()).then_some(Self {
            owner,
            range: start..end,
        })
    }

    /// Observe this allocation without retaining its payload.
    pub fn downgrade(&self) -> WeakValue {
        WeakValue {
            owner: Arc::downgrade(&self.owner),
            range: self.range.clone(),
        }
    }

    /// Return the payload length retained by this handle.
    /// This excludes allocation capacity, owner metadata, and allocator overhead.
    /// Several handles can refer to the same allocation. Do not sum them as distinct allocations.
    pub fn allocation_len(&self) -> usize {
        self.owner.len()
    }
}

impl AsRef<[u8]> for SharedValue {
    fn as_ref(&self) -> &[u8] {
        &self.owner[self.range.clone()]
    }
}

impl WeakValue {
    /// Return the owner metadata allocation retained after the payload is released.
    /// Observers of the same owner share these bytes. This excludes allocator overhead.
    pub fn owner_metadata_len(&self) -> usize {
        size_of::<Bytes>() + 2 * size_of::<usize>()
    }

    /// Retain the allocation for a read, or return None after all strong owners release it.
    /// Cache eviction can race with this call. A successful upgrade keeps the bytes alive.
    pub fn upgrade(&self) -> Option<SharedValue> {
        Some(SharedValue {
            owner: self.owner.upgrade()?,
            range: self.range.clone(),
        })
    }
}

impl From<Bytes> for SharedValue {
    fn from(bytes: Bytes) -> Self {
        Self::new(bytes)
    }
}
