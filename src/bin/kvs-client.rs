use clap::{arg, command, Arg, ArgMatches, Command};
use kvs::common::{Request, Response};
use serde::Deserialize;
use serde_json::de::{Deserializer, IoRead};
use std::{
    io::{BufReader, BufWriter, Write},
    net::TcpStream,
};

fn main() -> kvs::Result<()> {
    // create command line interface by using builder API in `clap`
    let addr_arg = Arg::new("ip_port")
        .long("addr")
        .value_name("IP-PORT")
        .help("IP address and port")
        .required(false);
    let matches = command!() // requires `cargo` feature
        .subcommands(&[
            Command::new("set")
                .about("Set the value of a string key to a string")
                .args(&[
                    arg!(<KEY> "A string key"),
                    arg!(<VALUE> "A string value"),
                    addr_arg.clone(),
                ]),
            Command::new("get")
                .about("Get the string value of a given string key")
                .args(&[arg!(<KEY> "A string key"), addr_arg.clone()]),
            Command::new("rm")
                .about("Remove a given string key")
                .args(&[arg!(<KEY> "A string key"), addr_arg.clone()]),
        ])
        .get_matches();

    match matches.subcommand() {
        Some(("set", sub_m)) => {
            let key = sub_m.get_one::<String>("KEY").unwrap();
            let value = sub_m.get_one::<String>("VALUE").unwrap();
            KvsClient::new(sub_m)?.set(key.clone(), value.clone())
        }
        Some(("get", sub_m)) => {
            let key = sub_m.get_one::<String>("KEY").unwrap();
            KvsClient::new(sub_m)?.get(key.clone())
        }
        Some(("rm", sub_m)) => {
            let key = sub_m.get_one::<String>("KEY").unwrap();
            KvsClient::new(sub_m)?.remove(key.clone())
        }
        _ => panic!(),
    }
}

struct KvsClient {
    reader: Deserializer<IoRead<BufReader<TcpStream>>>,
    writer: BufWriter<TcpStream>,
}

impl KvsClient {
    fn new(matches: &ArgMatches) -> kvs::Result<KvsClient> {
        let addr = matches
            .get_one::<String>("ip_port")
            .map_or(String::from("127.0.0.1:4000"), |x| x.clone());
        let stream =
            TcpStream::connect(addr).expect("unable to connect to {addr}");

        Ok(KvsClient {
            reader: Deserializer::from_reader(BufReader::new(
                stream.try_clone()?,
            )),
            writer: BufWriter::new(stream),
        })
    }

    fn set(&mut self, key: String, value: String) -> kvs::Result<()> {
        serde_json::to_writer(&mut self.writer, &Request::Set { key, value })?;
        self.writer.flush()?;

        // ignore status
        let Response::Status(_status) =
            Response::deserialize(&mut self.reader)?;
        Ok(())
    }

    fn get(&mut self, key: String) -> kvs::Result<()> {
        serde_json::to_writer(&mut self.writer, &Request::Get { key })?;
        self.writer.flush()?;

        let Response::Status(status) = Response::deserialize(&mut self.reader)?;
        println!("{status}");
        Ok(())
    }

    fn remove(&mut self, key: String) -> kvs::Result<()> {
        serde_json::to_writer(&mut self.writer, &Request::Remove { key })?;
        self.writer.flush()?;

        let Response::Status(status) = Response::deserialize(&mut self.reader)?;
        if !status.is_empty() {
            eprintln!("{status}");
            std::process::exit(1);
        }
        Ok(())
    }
}
