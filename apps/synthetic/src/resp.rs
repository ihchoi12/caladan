use clap::Arg;

use std::cmp::min;
use std::io;
use std::io::{Error, ErrorKind, Read};
use std::net::SocketAddrV4;
use std::str::FromStr;

use Buffer;
use Connection;
use LoadgenProtocol;
use Packet;
use Transport;

#[derive(Copy, Clone)]
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

pub enum ParseState {
    Done(usize, usize), // Header len, body len
    Partial(usize, Option<usize>),
}

impl ParseState {
    #[inline(always)]

    pub fn new() -> ParseState {
        ParseState::Partial(0, None)
    }
}

#[inline(always)]
fn find_subsequence(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    haystack
        .windows(needle.len())
        .position(|window| window == needle)
}

fn header_extract(state: ParseState, buf: &[u8]) -> io::Result<ParseState> {
    let (mut next_line_begin, mut content_len) = match state {
        ParseState::Done(_, _) => return Ok(state),
        ParseState::Partial(next_line_begin, content_len) => (next_line_begin, content_len),
    };

    if next_line_begin == buf.len() {
        return Ok(ParseState::Partial(next_line_begin, content_len));
    }

    loop {
        match find_subsequence(&buf[next_line_begin..], b"\r\n") {
            Some(idx) => {
                if idx == 0 {
                    if content_len.is_none() {
                        return Err(Error::new(ErrorKind::Other, "missing content-length"));
                    }
                    return Ok(ParseState::Done(next_line_begin + 2, content_len.unwrap()));
                }
                let header_ln = &buf[next_line_begin..idx + next_line_begin];

                let utfln = unsafe { std::str::from_utf8_unchecked(&header_ln[..]) };
                if next_line_begin == 0 {
                    match header_ln[0] {
                        b'+' | b'-' | b':' | b'$' | b'*' => (),
                        _ => return Err(Error::new(ErrorKind::Other, format!("got bad RESP response: {}", utfln))),
                    }
                }

                next_line_begin += idx + 2;
            },

            None => {
                return Ok(ParseState::Partial(next_line_begin, content_len));
            }
        }
    }
}

impl LoadgenProtocol for RespProtocol {
    fn uses_ordered_requests(&self) -> bool {
        true
    }

    fn gen_req(&self, _i: usize, _p: &Packet, buf: &mut Vec<u8>) {
        // PING

        buf.extend("*1\r\n$4\r\nPING\r\n".as_bytes());
    }

    fn read_response(&self, mut sock: &Connection, buf: &mut Buffer) -> io::Result<(usize, u64)> {
        let mut pstate = ParseState::new();

        if buf.data_size() == 0 {
            buf.try_shrink()?;
            let new_bytes = sock.read(buf.get_empty_buf())?;
            if new_bytes == 0 {
                return Err(Error::new(ErrorKind::UnexpectedEof, "eof"));
            }
            buf.push_data(new_bytes);
        }

        loop {
            let header_len;
            let body_len;

            match header_extract(pstate, buf.get_data())? {
                ParseState::Done(hlen, blen) => {
                    header_len = hlen;
                    body_len = blen;
                }

                ParseState::Partial(a, b) => {
                    pstate = ParseState::Partial(a, b);
                    buf.try_shrink()?;
                    let new_bytes = sock.read(buf.get_empty_buf())?;
                    if new_bytes == 0 {
                        return Err(Error::new(ErrorKind::UnexpectedEof, "eof"));
                    }
                    buf.push_data(new_bytes);
                    continue;
                }
            };

            /* drain socket if needed */
            let total_req_bytes = header_len + body_len;
            let curbytes = buf.data_size();
            buf.pull_data(min(curbytes, total_req_bytes));
            buf.try_shrink()?;
            if curbytes < total_req_bytes {
                let mut to_read = total_req_bytes - curbytes;
                while to_read > 0 {
                    let rlen = min(to_read, buf.get_free_space());
                    sock.read_exact(&mut buf.get_empty_buf()[..rlen])?;
                    to_read -= rlen;
                }
            }

            return Ok((0, 0));
        }
    }
}
