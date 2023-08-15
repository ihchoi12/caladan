use clap::Arg;
use rand::distributions::{Range, IndependentSample};
use rand::{OsRng, Rng, random, thread_rng};

use std::cell::RefCell;
use std::cmp::min;
use std::io;
use std::io::{Error, ErrorKind, Read};
use std::net::SocketAddrV4;
use std::str::from_utf8;
use std::sync::Arc;

use Buffer;
use Connection;
use LoadgenProtocol;
use Packet;
use Transport;

static READ_COMMANDS: [&str; 4] = [
    "PING",
    "EXISTS numbers",
    "LLEN numbers",
    "LRANGE numbers 0 -1"
];

static WRITE_COMMANDS: [&str; 2] = [
    "RPUSH numbers 9",
    "LPOP numbers"
];

const RW_RANGE: usize = 0;
const READ_RANGE: usize = 1;
const WRITE_RANGE: usize = 2;

const READ_PROBABILITY: u32 = 80;

pub struct RespProtocol {
    
}

impl RespProtocol {
    pub fn with_args(matches: &clap::ArgMatches, tport: Transport) -> Self {
        if let Transport::Udp = tport {
            panic!("udp is unsupported for resp");
        }

        RespProtocol {
            
        }
    }

    pub fn args<'a, 'b>() -> Vec<clap::Arg<'a, 'b>> {
        vec![]
    }
}

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

impl LoadgenProtocol for RespProtocol {
    fn uses_ordered_requests(&self) -> bool {
        true
    }

    fn gen_req(&self, _i: usize, _p: &Packet, buf: &mut Vec<u8>) {
        let mut rng = thread_rng();
        let command = if rng.gen_range(0, 100) < READ_PROBABILITY {
            READ_COMMANDS[rng.gen_range(0, READ_COMMANDS.len())]
        } else {
            WRITE_COMMANDS[rng.gen_range(0, WRITE_COMMANDS.len())]
        };

        let count = command.split(' ').count();
        buf.extend(format!("*{count}\r\n").as_bytes());
        for word in command.split(' ') {
            buf.extend(format!("${}\r\n{}\r\n", word.len(), word).as_bytes());
        }

        //eprintln!("Sending: {}", from_utf8(buf).unwrap());
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
