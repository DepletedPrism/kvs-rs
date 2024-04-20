use crate::{thread_pool::ThreadPool, Result};
use crossbeam::channel::{self, Receiver, Sender};
use std::thread;

enum ThreadMessage {
    Job(Box<dyn FnOnce() + Send + 'static>),
}

struct Worker {
    rx: Receiver<ThreadMessage>,
}

fn run_tasks(worker: Worker) {
    loop {
        match worker.rx.recv() {
            Ok(message) => match message {
                ThreadMessage::Job(job) => job(),
            },
            Err(_) => eprintln!("Worker disconnected; shutting down."),
        }
    }
}

impl Drop for Worker {
    fn drop(&mut self) {
        if thread::panicking() {
            let rx = self.rx.clone();
            if let Err(e) =
                thread::Builder::new().spawn(move || run_tasks(Worker { rx }))
            {
                eprintln!("unable to spawn a thread: {e}")
            }
        }
    }
}

pub struct SharedQueueThreadPool {
    sender: Sender<ThreadMessage>,
}

impl ThreadPool for SharedQueueThreadPool {
    fn new(threads: usize) -> Result<SharedQueueThreadPool> {
        let (sx, rx) = channel::unbounded::<ThreadMessage>();

        for _ in 0..threads {
            let rx = rx.clone();
            thread::Builder::new()
                .spawn(move || run_tasks(Worker { rx }))
                .expect("unable to spawn a thread");
        }

        Ok(SharedQueueThreadPool { sender: sx })
    }

    fn spawn<F>(&self, job: F)
    where
        F: FnOnce() + Send + 'static,
    {
        self.sender
            .send(ThreadMessage::Job(Box::new(job)))
            .expect("unable to send message to workers");
    }
}
