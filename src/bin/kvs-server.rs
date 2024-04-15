use clap::{command, Arg, ArgMatches};
use kvs::{
    common::{Request, Response},
    KvStore, KvsEngine, SledStore,
};
use serde_json::Deserializer;
use slog::{debug, error, info, o, Drain};
use std::{
    env,
    io::{BufReader, BufWriter, Read, Write},
    net::{TcpListener, TcpStream},
    path::PathBuf,
};

fn main() -> kvs::Result<()> {
    let decorator = slog_term::TermDecorator::new().build();
    let drain = slog_term::FullFormat::new(decorator).build().fuse();
    let drain = slog_async::Async::new(drain).build().fuse();

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

    KvsServer::new(&matches, slog::Logger::root(drain, o!()))?.run(addr)
}

struct KvsServer {
    logger: slog::Logger,
    store: Box<dyn KvsEngine>,
}

impl KvsServer {
    fn get_store(
        engine: &str,
        server: &slog::Logger,
    ) -> kvs::Result<Box<dyn KvsEngine>> {
        let identify = |path: PathBuf, current: &str| -> kvs::Result<()> {
            let path = path.join("identity");
            if let Ok(file) = std::fs::File::open(&path) {
                let mut id = String::new();
                let mut id_reader = BufReader::new(file);
                id_reader.read_to_string(&mut id)?;
                if id != current {
                    error!(
                        server,
                        "select `{current}` as engine, but pervious data is \
                        persisted with a different engine {id}"
                    );
                    std::process::exit(1);
                }
            } else {
                let mut id_writer =
                    BufWriter::new(std::fs::File::create_new(path)?);
                id_writer.write_all(current.as_bytes())?;
            }
            Ok(())
        };

        let path = env::current_dir()?.join(".kv_data");
        std::fs::create_dir_all(&path)?;
        Ok(match engine {
            "kvs" => {
                identify(path.clone(), "kvs")?;
                Box::new(KvStore::open(path.clone())?)
            }
            "sled" => {
                identify(path.clone(), "sled")?;
                Box::new(SledStore::open(path.clone())?)
            }
            _ => {
                error!(server, "select a nonexistent engine");
                std::process::exit(1)
            }
        })
    }

    fn new(
        matches: &ArgMatches,
        logger: slog::Logger,
    ) -> kvs::Result<KvsServer> {
        let engine = matches
            .get_one::<String>("engine_name")
            .map_or(String::from("kvs"), |x| x.clone());
        let version = std::env!("CARGO_PKG_VERSION");

        let store = Self::get_store(&engine, &logger)?;
        info!(logger, "version v{version} with engine {engine}.");

        Ok(KvsServer { logger, store })
    }

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
                    if let Err(e) = self.process(stream) {
                        error!(self.logger, "failed to serve client: {e}");
                    }
                }
                Err(e) => error!(self.logger, "failed at connection: {e}"),
            }
        }

        Ok(())
    }

    fn process(&mut self, stream: TcpStream) -> kvs::Result<()> {
        let reader = Deserializer::from_reader(BufReader::new(&stream));
        let mut writer = BufWriter::new(&stream);

        for request in reader.into_iter::<Request>() {
            let mut response = String::new();
            match request? {
                Request::Set { key, value } => {
                    self.store.set(key, value)?;
                }
                Request::Get { key } => {
                    let value = self.store.get(key)?;
                    response = match value {
                        Some(v) => v,
                        None => "Key not found".into(),
                    };
                }
                Request::Remove { key } => {
                    if self.store.remove(key).is_err() {
                        response = "Key not found".into()
                    }
                }
            }

            let response = Response::Status(response);
            serde_json::to_writer(&mut writer, &response)?;
            writer.flush()?;
            debug!(self.logger, "send response {:?}", response);
        }

        Ok(())
    }
}
