//! The producer buffer memory, Kafka's `buffer.memory`.
//!
//! Kafka's `RecordAccumulator.append` takes memory from its `BufferPool` for
//! each new batch, and `Sender` gives the memory back when the batch
//! completes. When the pool has too little free memory, `send` blocks for at
//! most the rest of `max.block.ms`, and then fails the record with
//! `BufferExhaustedException`.
//!
//! This pool counts bytes only. A batch holds a [`MemoryReservation`], and the
//! bytes return to the pool when the batch is dropped, after its records
//! complete.

use std::{sync::Arc, time::Duration};

use tokio::sync::{OwnedSemaphorePermit, Semaphore};

use crate::error::ProducerError;

/// The bytes that one batch holds. Dropping the reservation gives the bytes
/// back to the pool and wakes the oldest waiting send.
#[derive(Debug)]
pub(crate) struct MemoryReservation {
    _permit: OwnedSemaphorePermit,
}

/// A fixed amount of memory, handed out in the order of the requests.
#[derive(Debug)]
pub(crate) struct BufferPool {
    total: usize,
    poolable: usize,
    free: Arc<Semaphore>,
}

impl BufferPool {
    /// The largest `buffer_memory` the pool can count.
    pub(crate) const MAX_MEMORY: usize = Semaphore::MAX_PERMITS;

    /// Create a pool of `total` bytes. `poolable` is the batch size, which
    /// the exhausted error reports as Kafka's `poolableSize`.
    ///
    /// # Errors
    ///
    /// Returns an error when `total` is above [`Self::MAX_MEMORY`].
    pub(crate) fn new(total: usize, poolable: usize) -> Result<Self, String> {
        if total > Self::MAX_MEMORY {
            return Err(format!(
                "producer buffer memory: {total} is above the limit of {}",
                Self::MAX_MEMORY
            ));
        }
        Ok(Self {
            total,
            poolable,
            free: Arc::new(Semaphore::new(total)),
        })
    }

    pub(crate) const fn total(&self) -> usize {
        self.total
    }

    /// Reserve `size` bytes, and wait at most `max_time_to_block` for them.
    ///
    /// Kafka's `BufferPool.allocate` gives the memory at once when enough is
    /// free, and otherwise waits in the order of the requests.
    ///
    /// # Errors
    ///
    /// Returns [`ProducerError::InvalidConfig`] with Kafka's message when
    /// `size` is above the total memory, and
    /// [`ProducerError::BufferExhausted`] when the time ends first.
    pub(crate) async fn allocate(
        &self,
        size: usize,
        max_time_to_block: Duration,
    ) -> Result<MemoryReservation, ProducerError> {
        let permits = u32::try_from(size)
            .ok()
            .filter(|_| size <= self.total)
            .ok_or_else(|| {
                ProducerError::InvalidConfig(format!(
                    "Attempt to allocate {size} bytes, but there is a hard limit of {} on memory allocations.",
                    self.total
                ))
            })?;
        let acquire = Arc::clone(&self.free).acquire_many_owned(permits);
        match tokio::time::timeout(max_time_to_block, acquire).await {
            Ok(Ok(permit)) => Ok(MemoryReservation { _permit: permit }),
            // The pool never closes its semaphore.
            Ok(Err(_)) | Err(_) => Err(ProducerError::BufferExhausted {
                size,
                max_block: max_time_to_block,
                total: self.total,
                available: self.free.available_permits(),
                poolable: self.poolable,
            }),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const MIB: usize = 1024 * 1024;

    /// What one allocation gave, with the paused time it took.
    #[derive(Debug, PartialEq, Eq)]
    struct Allocation {
        waited: Duration,
        outcome: Result<(), String>,
    }

    async fn allocation(pool: &BufferPool, size: usize, max_block: Duration) -> Allocation {
        let started = tokio::time::Instant::now();
        let outcome = pool
            .allocate(size, max_block)
            .await
            .map(drop)
            .map_err(|error| error.to_string());
        Allocation {
            waited: started.elapsed(),
            outcome,
        }
    }

    #[test]
    fn pool_rejects_memory_above_its_limit() {
        assert2::assert!(BufferPool::new(BufferPool::MAX_MEMORY, 1).is_ok());
        assert2::assert!(
            BufferPool::new(BufferPool::MAX_MEMORY + 1, 1).map(|_| ())
                == Err(format!(
                    "producer buffer memory: {} is above the limit of {}",
                    BufferPool::MAX_MEMORY + 1,
                    BufferPool::MAX_MEMORY
                ))
        );
    }

    /// Kafka's `BufferPool.allocate`: memory that is free comes at once, a
    /// request larger than the pool fails at once, and a request that must
    /// wait fails after `maxTimeToBlockMs` with `BufferExhaustedException`.
    #[tokio::test(start_paused = true)]
    async fn allocate_gives_free_memory_and_times_out_when_full() {
        let pool = BufferPool::new(32 * MIB, 16 * MIB).expect("pool");
        let block = Duration::from_millis(250);

        let first = pool.allocate(16 * MIB, block).await.expect("first");
        let immediate = allocation(&pool, 16 * MIB, block).await;
        let second = pool.allocate(16 * MIB, block).await.expect("second");
        let exhausted = allocation(&pool, MIB, block).await;
        let too_large = allocation(&pool, 32 * MIB + 1, block).await;
        drop((first, second));

        assert2::assert!(
            [immediate, exhausted, too_large]
                == [
                    Allocation {
                        waited: Duration::ZERO,
                        outcome: Ok(()),
                    },
                    Allocation {
                        waited: block,
                        outcome: Err(
                            "Failed to allocate 1048576 bytes within the configured max \
                                      blocking time 250 ms. Total memory: 33554432 bytes. \
                                      Available memory: 0 bytes. Poolable size: 16777216 bytes"
                                .to_owned()
                        ),
                    },
                    Allocation {
                        waited: Duration::ZERO,
                        outcome: Err("invalid config: Attempt to allocate 33554433 bytes, but \
                                      there is a hard limit of 33554432 on memory allocations."
                            .to_owned()),
                    },
                ]
        );
    }

    /// Memory that a completed batch gives back goes to the waiting sends in
    /// the order of their requests. The small request does not take part of
    /// the freed memory ahead of the large request.
    #[tokio::test(start_paused = true)]
    async fn released_memory_wakes_the_oldest_waiter() {
        let pool = Arc::new(BufferPool::new(2 * MIB, MIB).expect("pool"));
        let held = pool.allocate(2 * MIB, Duration::ZERO).await.expect("held");

        let waiter = |size: usize| {
            let pool = Arc::clone(&pool);
            tokio::spawn(async move {
                let started = tokio::time::Instant::now();
                let reservation = pool.allocate(size, Duration::from_secs(1)).await;
                let waited = started.elapsed();
                // The batch holds its memory for a while before it completes.
                tokio::time::sleep(Duration::from_millis(50)).await;
                (waited, reservation.map(drop).is_ok())
            })
        };
        let large = waiter(2 * MIB);
        tokio::task::yield_now().await;
        let small = waiter(MIB);
        tokio::task::yield_now().await;

        tokio::time::sleep(Duration::from_millis(100)).await;
        drop(held);
        let large = large.await.expect("large waiter");
        let small = small.await.expect("small waiter");

        assert2::assert!(
            (large, small)
                == (
                    (Duration::from_millis(100), true),
                    (Duration::from_millis(150), true)
                )
        );
    }
}
