use clap::{arg, command, Command};

fn main() {
    // create command line interface by using builder API in clap
    let matches = command!() // requires `cargo` feature
        .subcommands(&[
            Command::new("set")
                .about("Set the value of a string key to a string")
                .args(&[arg!(<KEY> "A string key"), arg!(<VALUE> "A string value")]),
            Command::new("get")
                .about("Get the string value of a given string key")
                .arg(arg!(<KEY> "A string key")),
            Command::new("rm")
                .about("Remove a given string key")
                .arg(arg!(<KEY> "A string key")),
        ])
        .get_matches();

    match matches.subcommand() {
        Some(("set", _)) => {
            eprintln!("unimplemented");
            panic!();
        }
        Some(("get", _)) => {
            eprintln!("unimplemented");
            panic!();
        }
        Some(("rm", _)) => {
            eprintln!("unimplemented");
            panic!();
        }
        _ => panic!(),
    }
}
