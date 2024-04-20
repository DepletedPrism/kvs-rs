use crate::Result;

mod naive;
mod rayon;
mod share_queue;

pub use naive::NaiveThreadPool;
pub use rayon::RayonThreadPool;
pub use share_queue::SharedQueueThreadPool;

pub trait ThreadPool {
    fn new(threads: usize) -> Result<Self>
    where
        Self: Sized;

    fn spawn<F>(&self, job: F)
    where
        F: FnOnce() + Send + 'static;
}
