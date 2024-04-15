use clap::{arg, command, Command};
use kvs::{KvStore, KvsEngine};

fn main() -> kvs::Result<()> {
    // create command line interface by using builder API in clap
    let matches = command!() // requires `cargo` feature
        .subcommands(&[
            Command::new("set")
                .about("Set the value of a string key to a string")
                .args(&[
                    arg!(<KEY> "A string key"),
                    arg!(<VALUE> "A string value"),
                ]),
            Command::new("get")
                .about("Get the string value of a given string key")
                .arg(arg!(<KEY> "A string key")),
            Command::new("rm")
                .about("Remove a given string key")
                .arg(arg!(<KEY> "A string key")),
        ])
        .get_matches();

    let path = std::env::current_dir()?.join(".kv_data");
    match matches.subcommand() {
        Some(("set", sub_m)) => {
            let key = sub_m.get_one::<String>("KEY").unwrap();
            let value = sub_m.get_one::<String>("VALUE").unwrap();

            let mut store = KvStore::open(path)?;
            store.set(key.clone(), value.clone())
        }
        Some(("get", sub_m)) => {
            let key = sub_m.get_one::<String>("KEY").unwrap();

            let mut store = KvStore::open(path)?;
            let value = store.get(key.clone())?;
            match value {
                Some(v) => println!("{v}"),
                None => println!("Key not found"),
            }
            Ok(())
        }
        Some(("rm", sub_m)) => {
            let key = sub_m.get_one::<String>("KEY").unwrap();

            let mut store = KvStore::open(path)?;
            if store.remove(key.clone()).is_err() {
                println!("Key not found");
                std::process::exit(1);
            }
            Ok(())
        }
        _ => panic!(),
    }
}
