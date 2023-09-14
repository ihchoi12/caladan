/*#############################################################################
    IMPORTS
#############################################################################*/

use clap::Arg;
use itertools::Itertools;
use rand::{Rng, thread_rng};

use std::io::{self, Write};
use std::io::{Error, ErrorKind, Read};
use std::net::SocketAddrV4;
use std::str::{from_utf8, FromStr};

use Buffer;
use Connection;
use LoadgenProtocol;
use Packet;
use Transport;

use crate::backend::Backend;

/*#############################################################################
    CONSTANTS
#############################################################################*/

const STRING_KEY_BASE: &str = "string";
const LIST_KEY_BASE: &str = "list";

const WRITE_PROBABILITY: i32 = 20;

/*#############################################################################
    ENUMERATIONS
#############################################################################*/

enum RedisDataType {
    String(usize), // initial number of strings
    List(usize, usize), // initial number of lists, number of elements in each list
}

/*#############################################################################
    STRUCTURES
#############################################################################*/

pub struct RespProtocol {
    data_types: Vec<RedisDataType>,
    additional_servers: Vec<SocketAddrV4>,
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

        let dtype = buf.get_data()[0];
        buf.pull_data(1);
        match dtype {
            b'+' | b'-' | b':' => {
                self.read_line(sock, buf, false)?;
            },

            b'$' => {
                let len = self.read_line(sock, buf, true)?.1.unwrap();
                let len = &len[..len.len() - 2]; // Remove "\r\n".
                let mut len = from_utf8(len).unwrap().parse::<isize>().unwrap();
                if len != -1 {
                    while len > 0 {
                        let (bytes_read, _) = self.read_line(sock, buf, false)?; // bulk string length: we assume that the string length coincides with \r\n.
                        len -= bytes_read as isize;
                    }
                }
            },

            b'*' => {
                todo!("array return type");
            },

            c => panic!("Invalid RESP type: {}", c as char),
        };

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

        let additional_servers = if matches.is_present("redis-preload") {
            match matches.values_of("redis-preload") {
                None => panic!("No value for option `redis-preload`"),
                Some(servers) => servers.map(|e| SocketAddrV4::from_str(e).expect("Invalid server address")).collect_vec(),
            }
        } else {
            Vec::new()
        };

        if matches.is_present("redis-string") {
            let count = match matches.value_of("redis-string") {
                None => panic!("No value for option `redis-string`"),
                Some(val) => val.parse::<usize>().unwrap()
            };

            data_types.push(RedisDataType::String(count));
        }

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

        assert!(!data_types.is_empty(), "At least one Redis data type must be chosen");

        RespProtocol {
            data_types,
            additional_servers,
        }
    }

    pub fn args<'a, 'b>() -> Vec<clap::Arg<'a, 'b>> {
        vec![
            Arg::with_name("redis-string")
                .long("redis-string")
                .takes_value(true)
                .help("Enables string commands with value as the initial string count in Redis."),

            Arg::with_name("redis-list")
                .long("redis-list")
                .takes_value(true)
                .help("Enables list commands with value `a:b` where `a` is the initial list count and `b` is the initial element count in each list in Redis."),

            Arg::with_name("redis-preload")
                .long("redis-preload")
                .takes_value(true)
                .multiple(true)
                .help("Set up another Redis server using preload. The value of this option is the IP:Port of the new server."),
        ]
    }

    pub fn preload_servers(
        &self,
        backend: Backend,
        tport: Transport,
        addr: SocketAddrV4,
    ) {
        self.preload_server(backend, tport, addr);
        for &addr in self.additional_servers.iter() {
            self.preload_server(backend, tport, addr);
        }
    }

    fn preload_server(
        &self,
        backend: Backend,
        tport: Transport,
        addr: SocketAddrV4,
    ) {
        if let Transport::Udp = tport {
            panic!("udp is unsupported for resp");
        }

        let mut conn = backend.create_tcp_connection(None, addr).expect("Preload connection failed");
        let mut buf = vec![0u8; 4096];
        let mut buf = Buffer::new(&mut buf);

        for dtype in self.data_types.iter() {
            let cmds: Box<dyn Iterator<Item = String>> = match dtype {
                RedisDataType::String(count) => Box::new((0..*count)
                    .map(
                        |string| {
                            let key = format!("{STRING_KEY_BASE}{string}");
                            let value = format!("{string}");
                            format!("*3\r\n$3\r\nSET\r\n${}\r\n{}\r\n${}\r\n{}\r\n", key.len(), key, value.len(), value)
                        }
                    )
                ),

                RedisDataType::List(count, element_count) => Box::new((0..*count)
                    .cartesian_product(0..*element_count)
                    .map(
                        |(list, element)| {
                            let key = format!("{LIST_KEY_BASE}{list}");
                            let value = format!("{element}");
                            format!("*3\r\n$5\r\nRPUSH\r\n${}\r\n{}\r\n${}\r\n{}\r\n", key.len(), key, value.len(), value)
                        }
                    )
                ),
            };

            for cmd in cmds {
                if let Err(e) = conn.write_all(cmd.as_bytes()) {
                    panic!("Preload write failed: {}\nCommand:\n{}", e, cmd);
                }
                let response = self.read_line(&conn, &mut buf, true);
                if let Err(e) = response {
                    panic!("Preload read failed: {}\nCommand:\n{}", e, cmd);
                }
                let response = response.unwrap().1.unwrap();
                if response[0] == b'-' {
                    panic!("Preload command failed: {}\nCommand:\n{}", from_utf8(&response).unwrap(), cmd);
                }
            }
        }

        std::thread::sleep(std::time::Duration::from_millis(500));

        println!("[*] Redis server preload done");
    }

    /// Returns number of bytes read and, optionally, the bytes read, until a "\r\n".
    fn read_line(&self, mut sock: &Connection, buf: &mut Buffer, return_data: bool) -> io::Result<(usize, Option<Vec<u8>>)> {
        let mut bytes_read = 0;
        let mut line = None;
        if return_data { line = Some(Vec::<u8>::with_capacity(8)); }

        let mut found_carriage_return = false;

        loop {
            if buf.data_size() == 0 {
                buf.try_shrink()?;
                let new_bytes = sock.read(buf.get_empty_buf())?;
                if new_bytes == 0 {
                    return Err(Error::new(ErrorKind::UnexpectedEof, "eof"));
                }
                buf.push_data(new_bytes);
            }

            if found_carriage_return {
                if buf.get_data()[0] == b'\n' {
                    if return_data { line.as_mut().unwrap().push(b'\n'); }
                    bytes_read += 1;
                    buf.pull_data(1);
                    break;
                } else {
                    found_carriage_return = false;
                }
            }

            if let Some(index) = find_subsequence(buf.get_data(), b"\r") {
                if return_data { line.as_mut().unwrap().extend_from_slice(&buf.get_data()[..index + 1]); }
                bytes_read += index + 1;
                buf.pull_data(index + 1);
                found_carriage_return = true;
                continue;
            }

            if return_data { line.as_mut().unwrap().extend_from_slice(buf.get_data()); }
            bytes_read += buf.data_size();
            buf.pull_data(buf.data_size());
        }

        Ok((bytes_read, line))
    }
}

impl RedisDataType {
    fn generate_random_command(&self, rng: &mut impl Rng, is_write_command: bool) -> String {
        match self {
            RedisDataType::String(count) => {
                let key = format!("{}{}", STRING_KEY_BASE, rng.gen_range(0, *count));
                if is_write_command { match rng.gen_range(0, 2) {
                    // SET
                    0 => format_command(&["SET", &key, &format!("{}", rng.gen::<usize>())]),

                    // GETDEL
                    1 => format_command(&["GETDEL", &key]),

                    _ => unreachable!(),
                } } else { match rng.gen_range(0, 1) {
                    // GET
                    0 => format_command(&["GET", &key]),

                    _ => unreachable!(),
                } }
            },

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

fn format_command(args: &[&str]) -> String {
    args.iter().fold(format!("*{}\r\n", args.len()), |cmd, arg| format!("{}${}\r\n{}\r\n", cmd, arg.len(), arg))
}