/*#############################################################################
    IMPORTS
#############################################################################*/

use clap::Arg;
use itertools::Itertools;
use rand::{Rng, thread_rng};

use std::io::{self, Write};
use std::io::{Error, ErrorKind, Read};
use std::net::SocketAddrV4;
use std::str::from_utf8;

use Buffer;
use Connection;
use LoadgenProtocol;
use Packet;
use Transport;

use crate::backend::Backend;

/*#############################################################################
    CONSTANTS
#############################################################################*/

const LIST_KEY_BASE: &str = "list";

const WRITE_PROBABILITY: i32 = 20;

/*#############################################################################
    ENUMERATIONS
#############################################################################*/

enum RedisDataType {
    List(usize, usize), // number of lists, number of elements in each list, the list key base
}

/*#############################################################################
    STRUCTURES
#############################################################################*/

pub struct RespProtocol {
    data_types: Vec<RedisDataType>,
}

/*#############################################################################
    STANDARD LIBRARY TRAIT IMPLEMENTATIONS
#############################################################################*/



/*#############################################################################
    TRAIT IMPLEMENTATIONS
#############################################################################*/

impl LoadgenProtocol for RespProtocol {
    fn uses_ordered_requests(&self) -> bool {
        true
    }

    fn gen_req(&self, _i: usize, _p: &Packet, buf: &mut Vec<u8>) {
        let mut rng = thread_rng();
        let dtype = &self.data_types[rng.gen_range(0, self.data_types.len())];
        let is_write_command = rng.gen_range(0, 100) < WRITE_PROBABILITY;
        let command = dtype.generate_random_command(&mut rng, is_write_command);

        //eprintln!("Sending: {}", command);

        buf.extend(command.as_bytes());
    }

    fn read_response(&self, mut sock: &Connection, buf: &mut Buffer) -> io::Result<(usize, u64)> {
        if buf.data_size() == 0 {
            buf.try_shrink()?;
            let new_bytes = sock.read(buf.get_empty_buf())?;
            if new_bytes == 0 {
                return Err(Error::new(ErrorKind::UnexpectedEof, "eof"));
            }
            buf.push_data(new_bytes);
        }

        check_good_response(buf.get_data())?;

        //eprintln!("Received: {}", from_utf8(buf.get_data()).unwrap());

        buf.pull_data(buf.data_size());
        buf.try_shrink()?;

        return Ok((0, 0));
    }
}

/*#############################################################################
    IMPLEMENTATIONS
#############################################################################*/

impl RespProtocol {
    pub fn with_args(matches: &clap::ArgMatches, tport: Transport) -> Self {
        if let Transport::Udp = tport {
            panic!("udp is unsupported for resp");
        }

        let mut data_types = Vec::new();

        if matches.is_present("redis-list") {
            let (count, element_count) = match matches.value_of("redis-list") {
                None => panic!("No value for option `redis-list`"),
                Some(val) => {
                    let vals = val.split(':').collect::<Vec<&str>>();
                    match vals.len() {
                        2 => (vals[0].parse::<usize>().unwrap(), vals[1].parse::<usize>().unwrap()),
                        _ => panic!("Invalid value format for `redis-list`"),
                    }
                }
            };

            data_types.push(RedisDataType::List(count, element_count));
        }

        RespProtocol {
            data_types
        }
    }

    pub fn args<'a, 'b>() -> Vec<clap::Arg<'a, 'b>> {
        vec![
            Arg::with_name("redis_list")
                .long("redis_list")
                .takes_value(true)
                .help("Signals Redis to use lists with value `a:b` where `a` is the list count and `b` is the element count in each list."),
        ]
    }

    pub fn preload_server(
        &self,
        backend: Backend,
        tport: Transport,
        addr: SocketAddrV4,
    ) {
        if let Transport::Udp = tport {
            panic!("udp is unsupported for resp");
        }

        let mut conn = backend.create_tcp_connection(None, addr).expect("Preload connection failed");
        let mut buf = Vec::with_capacity(4096);

        for dtype in self.data_types.iter() {
            let cmds = match dtype {
                RedisDataType::List(count, element_count) => (0..*count)
                    .cartesian_product(0..*element_count)
                    .map(
                        |(list, element)| {
                            let key = format!("{LIST_KEY_BASE}{list}");
                            let value = format!("{element}");
                            format!("*3\r\n$5\r\nRPUSH\r\n${}\r\n{}\r\n${}\r\n{}\r\n", key.len(), key, value.len(), value)
                        }
                    ),
            };

            for cmd in cmds {
                if let Err(e) = conn.write_all(cmd.as_bytes()) {
                    panic!("Preload write failed: {}\nCommand:\n{}", e, cmd);
                }
                buf.clear();
                if let Err(e) = conn.read(&mut buf) {
                    panic!("Preload read failed: {}\nCommand:\n{}", e, cmd);
                }
                if buf[0] == b'-' {
                    panic!("Preload command failed: {}\nCommand:\n{}", from_utf8(&buf).unwrap(), cmd);
                }
            }
        }
    }
}

impl RedisDataType {
    fn generate_random_command(&self, rng: &mut impl Rng, is_write_command: bool) -> String {
        match self {
            RedisDataType::List(count, _) => {
                let key = format!("{}{}", LIST_KEY_BASE, rng.gen_range(0, *count));
                if is_write_command { match rng.gen_range(0, 2) {
                    // RPUSH
                    0 => format_command(&["RPUSH", &key, &format!("{}", rng.gen::<usize>())]),

                    // LPOP
                    1 => format_command(&["LPOP", &key]),

                    _ => unreachable!(),
                } } else { match rng.gen_range(0, 2) {
                    // EXISTS
                    0 => format_command(&["EXISTS", &key]),

                    // LLEN
                    1 => format_command(&["LLEN", &key]),

                    _ => unreachable!(),
                } }
            },
        }
    }
}

/*#############################################################################
    HELPER FUNCTIONS
#############################################################################*/

#[inline(always)]
fn find_subsequence(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    haystack
        .windows(needle.len())
        .position(|window| window == needle)
}

fn check_good_response(buf: &[u8]) -> io::Result<()> {
    match buf[0] {
        b'+' | b'-' | b':' | b'$' | b'*' => Ok(()),
        _ => Err(Error::new(ErrorKind::Other, format!("got bad RESP response: {}", from_utf8(buf).unwrap()))),
    }
}

fn format_command(args: &[&str]) -> String {
    args.iter().fold(format!("*{}\r\n", args.len()), |cmd, arg| format!("{}${}\r\n{}\r\n", cmd, arg.len(), arg))
}