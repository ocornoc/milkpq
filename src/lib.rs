//! crate docs

#![warn(
    clippy::all, clippy::pedantic, clippy::clippy::cargo_common_metadata,
    missing_crate_level_docs, missing_debug_implementations, missing_doc_code_examples,
    missing_docs,
)]
#![allow(clippy::clippy::must_use_candidate)]

use std::collections::BinaryHeap;
use std::cell::UnsafeCell;
use std::iter::FromIterator;
use std::fmt::{Debug, Formatter, Result as FmtResult};
use std::sync::atomic::{spin_loop_hint, AtomicBool, Ordering::{Relaxed, Release}};
use ref_thread_local::{ref_thread_local, RefThreadLocal};
use rand_distr::Uniform;
use rand::prelude::*;

ref_thread_local! {
    static managed PRNG: SmallRng = SmallRng::from_entropy();
}

/// docs
pub struct MilkPQ<T: Ord> {
    queues: Box<[Queue<T>]>,
    dist: Uniform<usize>,
}

impl<T: Ord + Clone> Clone for MilkPQ<T> {
    fn clone(&self) -> Self {
        MilkPQ { queues: self.queues.clone(), dist: self.dist }
    }

    fn clone_from(&mut self, source: &Self) {
        self.queues.clone_from(&source.queues);
        self.dist = source.dist;
    }
}

impl<T: Ord> FromIterator<T> for MilkPQ<T> {
    fn from_iter<I: IntoIterator<Item = T>>(iter: I) -> Self {
        let iter = iter.into_iter();
        let pq = MilkPQ::with_capacity(iter.size_hint().0);
        
        for t in iter {
            pq.push(t);
        }

        pq
    }
}

impl<T: Ord> From<MilkPQ<T>> for Vec<T> {
    fn from(pq: MilkPQ<T>) -> Self {
        let mut vec = Vec::new();

        for pq in pq.queues.into_vec() {
            vec.extend(pq);
        }

        vec
    }
}

impl<T: Ord> IntoIterator for MilkPQ<T> {
    type Item = T;
    type IntoIter = std::vec::IntoIter<T>;

    fn into_iter(self) -> Self::IntoIter {
        Vec::into_iter(self.into())
    }
}

impl<T: Ord> Extend<T> for MilkPQ<T> {
    fn extend<I: IntoIterator<Item = T>>(&mut self, iter: I) {
        self.extend_ref(iter);
    }
}

impl<T: Ord> Default for MilkPQ<T> {
    fn default() -> Self {
        MilkPQ::new()
    }
}

impl<T: Ord + Debug> Debug for MilkPQ<T> {
    fn fmt(&self, f: &mut Formatter) -> FmtResult {
        f.debug_list().entries(self.queues.as_ref()).finish()
    }
}

impl<T: Ord> MilkPQ<T> {
    /// Create a new [`MilkPQ`] priority queue.
    pub fn new() -> Self {
        Self::with_queues(num_cpus::get() * 4)
    }

    /// Create a new [`MilkPQ`] with each subqueue having `cap` capacity.
    pub fn with_capacity(cap: usize) -> Self {
        Self::with_capacity_and_queues(cap, num_cpus::get() * 4)
    }

    /// Create a new [`MilkPQ`] with a given number of subqueues.
    pub fn with_queues(limit: usize) -> Self {
        let queues = std::iter::repeat_with(|| Queue::new(BinaryHeap::new()))
            .take(limit)
            .collect::<Vec<_>>()
            .into_boxed_slice();
        
        MilkPQ { queues, dist: Uniform::new(0, limit) }
    }

    /// Create a new [`MilkPQ`] with a given capacity and subqueue count.
    ///
    /// See [`with_capacity()`] and [`with_queues()`], as this is just a
    /// combination of the two.
    pub fn with_capacity_and_queues(cap: usize, limit: usize) -> Self {
        let queues = std::iter::repeat_with(|| Queue::new(BinaryHeap::with_capacity(cap)))
            .take(limit)
            .collect::<Vec<_>>()
            .into_boxed_slice();
        
        MilkPQ { queues, dist: Uniform::new(0, limit) }
    }

    /// Push an element into a subqueue.
    pub fn push(&self, mut t: T) {
        let mut i = PRNG.borrow_mut().sample(self.dist);
        
        while let Err(t2) = self.queues[i].try_push(t) {
            t = t2;
            i = PRNG.borrow_mut().sample(self.dist);
            spin_loop_hint();
        }
    }

    /// Pop the maximum element in a priority subqueue.
    ///
    /// This works by trying to lock a random subqueue and popping an element
    /// from that. Thus, this can have spurious [`None`]s when some subqueues
    /// are empty while others are not. For a function that is guaranteed to
    /// pop an element if any exist in any subqueues, see [`strong_pop()`].
    pub fn pop(&self) -> Option<T> {
        let mut i = PRNG.borrow_mut().sample(self.dist);
        let mut t;

        while {t = self.queues[i].try_pop(); t.is_err()} {
            i = PRNG.borrow_mut().sample(self.dist);
            spin_loop_hint();
        }

        t.unwrap()
    }

    /// Pop an element from the priority queue, but non-spuriously.
    ///
    /// This will check every subqueue until it finds some element (*not
    /// necessarily the maximum in the whole structure*) and returns it, or
    /// will return [`None`]. Thus, it returns [`None`] iff the structure is
    /// truly empty.
    pub fn strong_pop(&self) -> Option<T> {
        let mut t;

        for queue in self.queues.as_ref() {
            while {t = queue.try_pop(); t.is_err()} {
                spin_loop_hint();
            }

            let t = t.unwrap();
            if t.is_some() {
                return t;
            }
        }

        None
    }

    /// Turns `self` into a descending sorted [`Vec`].
    pub fn into_sorted_vec(self) -> Vec<T> {
        let mut vec = Vec::from(self);
        vec.sort_unstable_by(|l, r| l.cmp(r).reverse());
        vec
    }

    /// Clears all subqueues in the structure.
    pub fn clear(&self) {
        for queue in self.queues.as_ref() {
            queue.clear();
        }
    }

    /// Empty the contents of `self` into a [`Vec`] and leave `self` empty.
    pub fn drain(&mut self) -> Vec<T> {
        let mut vec = Vec::new();

        for queue in self.queues.as_mut() {
            vec.extend(queue.take())
        }

        vec
    }

    /// Extend `self` using an [`IntoIterator`].
    ///
    /// Exactly like [`Extend`], except it doesn't mutably borrow `self`.
    pub fn extend_ref<I: IntoIterator<Item = T>>(&self, iter: I) {
        for t in iter {
            self.push(t);
        }
    }
}

struct Queue<T: Ord> {
    pq: UnsafeCell<BinaryHeap<T>>,
    cas_lock: AtomicBool,
}

unsafe impl<T: Ord + Send> Send for Queue<T> {}
unsafe impl<T: Ord + Sync> Sync for Queue<T> {}

impl<T: Ord> IntoIterator for Queue<T> {
    type Item = T;
    type IntoIter = std::collections::binary_heap::IntoIter<T>;

    fn into_iter(self) -> Self::IntoIter {
        self.pq.into_inner().into_iter()
    }
}

impl<T: Ord + Clone> Clone for Queue<T> {
    fn clone(&self) -> Self {
        while self.cas_lock.compare_exchange_weak(false, true, Release, Relaxed).is_err() {
            spin_loop_hint();
        }

        let pq = UnsafeCell::new(unsafe { self.pq.get().as_ref() }.unwrap().clone());
        let cas_lock = AtomicBool::new(false);
        self.cas_lock.store(false, Release);
        Queue { pq, cas_lock }
    }

    fn clone_from(&mut self, source: &Self) {
        while source.cas_lock.compare_exchange_weak(false, true, Release, Relaxed).is_err() {
            spin_loop_hint();
        }

        unsafe { self.pq.get().as_mut() }
            .unwrap()
            .clone_from(unsafe { source.pq.get().as_ref() }.unwrap());

        source.cas_lock.store(false, Release);
    }
}

impl<T: Ord + Debug> Debug for Queue<T> {
    fn fmt(&self, f: &mut Formatter) -> FmtResult {
        while self.cas_lock.compare_exchange_weak(false, true, Release, Relaxed).is_err() {
            spin_loop_hint();
        }

        let fmt = unsafe { self.pq.get().as_ref() }.unwrap().fmt(f);
        self.cas_lock.store(false, Release);
        fmt
    }
}

impl<T: Ord> Queue<T> {
    fn new(pq: BinaryHeap<T>) -> Self {
        Queue {
            pq: UnsafeCell::new(pq),
            cas_lock: AtomicBool::new(false),
        }
    }

    #[must_use = "must check if CAS failed"]
    fn try_push(&self, t: T) -> Result<(), T> {
        match self.cas_lock.compare_exchange_weak(false, true, Release, Relaxed) {
            Ok(_) => {
                unsafe { self.pq.get().as_mut() }.unwrap().push(t);
                self.cas_lock.store(false, Release);
                Ok(())
            }
            Err(_) => Err(t),
        }
    }

    #[must_use = "must check if CAS failed"]
    fn try_pop(&self) -> Result<Option<T>, ()> {
        match self.cas_lock.compare_exchange_weak(false, true, Release, Relaxed) {
            Ok(_) => {
                let r = unsafe { self.pq.get().as_mut() }.unwrap().pop();
                self.cas_lock.store(false, Release);
                Ok(r)
            }
            Err(_) => Err(()),
        }
    }

    fn clear(&self) {
        while self.cas_lock.compare_exchange_weak(false, true, Release, Relaxed).is_err() {
            spin_loop_hint();
        }

        unsafe { self.pq.get().as_mut() }.unwrap().clear();
        self.cas_lock.store(false, Release);
    }

    fn take(&mut self) -> BinaryHeap<T> {
        let pq = unsafe { self.pq.get().as_mut() }.unwrap();
        let new = BinaryHeap::with_capacity(pq.capacity());
        std::mem::replace(pq, new)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::Ordering;
    use super::*;

    #[test]
    fn try_push() {
        let q = Queue::new(BinaryHeap::new());
        assert_eq!(unsafe { q.pq.get().as_ref() }.unwrap().len(), 0);
        assert_eq!(q.try_push(1), Ok(()));
        assert_eq!(unsafe { q.pq.get().as_ref() }.unwrap().len(), 1);
        q.cas_lock.store(true, Ordering::Release);
        assert_eq!(q.try_push(2), Err(2));
        assert_eq!(unsafe { q.pq.get().as_ref() }.unwrap().len(), 1);
        q.cas_lock.store(false, Ordering::Release);
        assert_eq!(q.try_push(2), Ok(()));
        assert_eq!(unsafe { q.pq.get().as_ref() }.unwrap().len(), 2);
    }

    #[test]
    fn try_pop() {
        let mut bheap = BinaryHeap::new();
        bheap.push(1);
        bheap.push(2);
        let q = Queue::new(bheap);
        assert_eq!(unsafe { q.pq.get().as_ref() }.unwrap().len(), 2);
        assert_eq!(q.try_pop(), Ok(Some(2)));
        assert_eq!(unsafe { q.pq.get().as_ref() }.unwrap().len(), 1);
        q.cas_lock.store(true, Ordering::Release);
        assert_eq!(q.try_pop(), Err(()));
        assert_eq!(unsafe { q.pq.get().as_ref() }.unwrap().len(), 1);
        q.cas_lock.store(false, Ordering::Release);
        assert_eq!(q.try_pop(), Ok(Some(1)));
        assert_eq!(unsafe { q.pq.get().as_ref() }.unwrap().len(), 0);
        assert_eq!(q.try_pop(), Ok(None));
        assert_eq!(unsafe { q.pq.get().as_ref() }.unwrap().len(), 0);
    }

    #[test]
    fn take() {
        let mut bheap = BinaryHeap::new();
        bheap.push(1);
        bheap.push(2);
        bheap.push(0);
        let mut q = Queue::new(bheap.clone());
        assert_eq!(bheap.into_sorted_vec(), q.take().into_sorted_vec());
        assert_eq!(unsafe { q.pq.get().as_ref() }.unwrap().len(), 0);
    }

    #[test]
    fn queue_clear() {
        let mut bheap = BinaryHeap::new();
        bheap.push(1);
        bheap.push(2);
        let q = Queue::new(bheap);
        q.clear();
        assert_eq!(unsafe { q.pq.get().as_ref() }.unwrap().len(), 0);
    }

    #[test]
    fn into_sorted_vec() {
        let q = MilkPQ::new();
        let mut vs = (0..100).collect::<Vec<_>>();
        vs.shuffle(&mut *PRNG.borrow_mut());
        q.extend_ref(vs);
        vs = q.into_sorted_vec();
        assert_eq!(vs, (0..100).rev().collect::<Vec<_>>());
    }

    #[test]
    fn strong_pop() {
        let q = MilkPQ::new();
        q.push(1);
        q.push(2);
        assert!(q.strong_pop().is_some());
        assert!(q.strong_pop().is_some());
        assert!(q.strong_pop().is_none());
    }
}
