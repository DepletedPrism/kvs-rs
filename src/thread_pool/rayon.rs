use crate::{thread_pool::ThreadPool, Result};

pub struct RayonThreadPool {
    pool: rayon::ThreadPool,
}

impl ThreadPool for RayonThreadPool {
    fn new(threads: usize) -> Result<RayonThreadPool> {
        let pool = rayon::ThreadPoolBuilder::new()
            .num_threads(threads)
            .spawn_handler(|thread| {
                std::thread::spawn(|| thread.run());
                Ok(())
            })
            .build()
            .expect("unable to crete thread pool using `rayon`");
        Ok(RayonThreadPool { pool })
    }

    fn spawn<F>(&self, job: F)
    where
        F: FnOnce() + Send + 'static,
    {
        self.pool.install(|| {
            job();
        });
    }
}
