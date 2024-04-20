use clap::{command, Arg};
use kvs::{
    common::{Request, Response},
    thread_pool::{NaiveThreadPool, ThreadPool},
    KvStore, KvsEngine, SledStore,
};
use serde_json::Deserializer;
use slog::{debug, error, info, o, Drain};
use std::{
    env,
    io::{BufReader, BufWriter, Read, Write},
    net::{TcpListener, TcpStream},
};

fn main() -> kvs::Result<()> {
    let decorator = slog_term::TermDecorator::new().build();
    let drain = slog_term::FullFormat::new(decorator).build().fuse();
    let drain = slog_async::Async::new(drain).build().fuse();

    let server = slog::Logger::root(drain, o!());

    let matches = command!()
        .about("Set IP address, port and which engine to run")
        .args(&[
            Arg::new("ip_port")
                .long("addr")
                .value_name("IP-PORT")
                .help("IP address and port")
                .required(false),
            Arg::new("engine_name")
                .long("engine")
                .value_name("ENGINE-NAME")
                .help("Name of used engine")
                .required(false),
        ])
        .get_matches();
    let addr = matches
        .get_one::<String>("ip_port")
        .map_or(String::from("127.0.0.1:4000"), |x| x.clone());
    let engine = matches
        .get_one::<String>("engine_name")
        .map_or(String::from("kvs"), |x| x.clone());
    let version = std::env!("CARGO_PKG_VERSION");

    let path = env::current_dir()?.join(".kv_data");
    let cpus = num_cpus::get();

    std::fs::create_dir_all(&path)?;
    match engine.as_str() {
        "kvs" => {
            identify_engine(path.as_path(), "kvs", &server)?;
            info!(server, "version v{version} with engine {engine}.");
            KvsServer {
                logger: server,
                store: KvStore::open(path.clone())?,
                pool: NaiveThreadPool::new(cpus)?,
            }
            .run(addr)
        }
        "sled" => {
            identify_engine(path.as_path(), "sled", &server)?;
            info!(server, "version v{version} with engine {engine}.");
            KvsServer {
                logger: server,
                store: SledStore::open(path.clone())?,
                pool: NaiveThreadPool::new(cpus)?,
            }
            .run(addr)
        }
        _ => {
            error!(server, "select a nonexistent engine");
            std::process::exit(1)
        }
    }
}

fn identify_engine(
    path: &std::path::Path,
    current: &str,
    logger: &slog::Logger,
) -> kvs::Result<()> {
    let path = path.join("identity");
    if let Ok(file) = std::fs::File::open(&path) {
        let mut id = String::new();
        let mut id_reader = BufReader::new(file);
        id_reader.read_to_string(&mut id)?;
        if id != current {
            error!(
                logger,
                "select `{current}` as engine, but pervious data is persisted \
                with a different engine {id}"
            );
            std::process::exit(1);
        }
    } else {
        let mut id_writer = BufWriter::new(std::fs::File::create_new(path)?);
        id_writer.write_all(current.as_bytes())?;
    }
    Ok(())
}

struct KvsServer<E: KvsEngine, P: ThreadPool> {
    logger: slog::Logger,
    store: E,
    pool: P,
}

impl<E: KvsEngine, P: ThreadPool> KvsServer<E, P> {
    fn run<A>(&mut self, addr: A) -> kvs::Result<()>
    where
        A: std::net::ToSocketAddrs + std::fmt::Display,
    {
        let listener = TcpListener::bind(&addr).unwrap_or_else(|_| {
            panic!("unable to create TCP listener at {addr}")
        });
        info!(self.logger, "server starts at {addr}.");

        for stream in listener.incoming() {
            match stream {
                Ok(stream) => {
                    let store = self.store.clone();
                    let logger = self.logger.clone();
                    self.pool.spawn(move || {
                        if let Err(e) = process(store, &logger, stream) {
                            error!(logger, "failed to serve client: {e}");
                        }
                    });
                }
                Err(e) => error!(self.logger, "failed at connection: {e}"),
            }
        }

        Ok(())
    }
}

fn process<E: KvsEngine>(
    store: E,
    logger: &slog::Logger,
    stream: TcpStream,
) -> kvs::Result<()> {
    let reader = Deserializer::from_reader(BufReader::new(&stream));
    let mut writer = BufWriter::new(&stream);

    for request in reader.into_iter::<Request>() {
        let mut response = String::new();
        match request? {
            Request::Set { key, value } => {
                store.set(key, value)?;
            }
            Request::Get { key } => {
                let value = store.get(key)?;
                response = match value {
                    Some(v) => v,
                    None => "Key not found".into(),
                };
            }
            Request::Remove { key } => {
                if store.remove(key).is_err() {
                    response = "Key not found".into()
                }
            }
        }

        let response = Response::Status(response);
        serde_json::to_writer(&mut writer, &response)?;
        writer.flush()?;
        debug!(logger, "send response {:?}", response);
    }

    Ok(())
}
