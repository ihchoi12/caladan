#![feature(test)]
#[macro_use]
extern crate clap;

extern crate byteorder;
extern crate dns_parser;
extern crate itertools;
extern crate libc;
extern crate net2;
extern crate rand;
extern crate rand_distr;
extern crate rand_mt;
extern crate shenango;
extern crate test;

extern crate arrayvec;

use arrayvec::ArrayVec;

use std::collections::{BTreeMap, HashMap};
use std::f32::INFINITY;
use std::{io, result};
use std::io::{Error, ErrorKind, Read, Write};
use std::net::{Ipv4Addr, SocketAddrV4};
use std::slice;
use std::str::FromStr;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use clap::{App, Arg};
use itertools::{Itertools, Either};
use rand_distr::{Normal};
use rand::{Rng, SeedableRng, rngs::StdRng};
use rand::seq::SliceRandom;
use rand_mt::Mt64;
use shenango::udp::UdpSpawner;

mod backend;
use backend::*;

mod lockstep;

mod payload;
use payload::{Payload, SyntheticProtocol, PAYLOAD_SIZE};

#[derive(Default)]
pub struct Packet {
    work_iterations: u64,
    randomness: u64,
    target_start: Duration,
    actual_start: Option<Duration>,
    completion_time_ns: AtomicU64,
    completion_server_tsc: Option<u64>,
    completion_time: Option<Duration>,
    server_port: Option<u16>,
    queue_len: Option<u32>,
}

mod fakework;
use fakework::FakeWorker;

mod memcached;
use memcached::MemcachedProtocol;

mod dns;
use dns::DnsProtocol;

mod reflex;
use reflex::ReflexProtocol;

mod http;
use http::HttpProtocol;

mod resp;
use resp::RespProtocol;

use std::fs::{File, OpenOptions};

static mut EXPTID: Option<String> = None;
static mut ONOFF: Option<String> = None;


mod distribution;
use distribution::Distribution;

static mut LATENCY_TRACE_RESULTS: Vec<(u64, u64)> = Vec::new(); // actual_start, latency


arg_enum! {
#[derive(Copy, Clone)]
pub enum Transport {
    Udp,
    Tcp,
}}

pub struct Buffer<'a> {
    buf: &'a mut [u8],
    head: usize,
    tail: usize,
}

impl<'a> Buffer<'a> {
    pub fn new(inbuf: &mut [u8]) -> Buffer {
        Buffer {
            buf: inbuf,
            head: 0,
            tail: 0,
        }
    }

    pub fn data_size(&self) -> usize {
        self.head - self.tail
    }

    pub fn get_data(&self) -> &[u8] {
        &self.buf[self.tail..self.head]
    }

    pub fn push_data(&mut self, size: usize) {
        self.head += size;
        assert!(self.head <= self.buf.len());
    }

    pub fn pull_data(&mut self, size: usize) {
        assert!(size <= self.data_size());
        self.tail += size;
    }

    pub fn get_free_space(&self) -> usize {
        self.buf.len() - self.head
    }

    pub fn get_empty_buf(&mut self) -> &mut [u8] {
        &mut self.buf[self.head..]
    }

    pub fn try_shrink(&mut self) -> io::Result<()> {
        if self.data_size() == 0 {
            self.head = 0;
            self.tail = 0;
            return Ok(());
        }

        if self.head < self.buf.len() {
            return Ok(());
        }

        if self.data_size() == self.buf.len() {
            return Err(Error::new(ErrorKind::Other, "Need larger buffers"));
        }

        self.buf.as_mut().copy_within(self.tail..self.head, 0);
        self.head = self.data_size();
        self.tail = 0;
        Ok(())
    }
}

trait LoadgenProtocol: Send + Sync {
    fn gen_req(&self, i: usize, p: &Packet, buf: &mut Vec<u8>);
    fn uses_ordered_requests(&self) -> bool;
    fn read_response(&self, sock: &Connection, scratch: &mut Buffer) -> io::Result<(usize, u64)>;
}

arg_enum! {
#[derive(Copy, Clone)]
enum OutputMode {
    Silent,
    Normal,
    Buckets,
    Trace,
    Live
}}

fn duration_to_ns(duration: Duration) -> u64 {
    duration.as_secs() * 1000_000_000 + duration.subsec_nanos() as u64
}

fn run_linux_udp_server(
    backend: Backend,
    addr: SocketAddrV4,
    nthreads: usize,
    worker: Arc<FakeWorker>,
) {
    let join_handles: Vec<_> = (0..nthreads)
        .map(|_| {
            let worker = worker.clone();
            backend.spawn_thread(move || {
                let socket = backend.create_udp_connection(addr, None).unwrap();
                println!("Bound to address {}", socket.local_addr());
                let mut buf = vec![0; 4096];
                loop {
                    let (len, remote_addr) = socket.recv_from(&mut buf[..]).unwrap();
                    let payload = Payload::deserialize(&mut &buf[..len]).unwrap();
                    worker.work(payload.work_iterations, payload.randomness);
                    socket.send_to(&buf[..len], remote_addr).unwrap();
                }
            })
        })
        .collect();

    for j in join_handles {
        j.join().unwrap();
    }
}

fn socket_worker(socket: &mut Connection, worker: Arc<FakeWorker>) {
    let mut v = vec![0; PAYLOAD_SIZE];
    let mut r = || {
        socket.read_exact(&mut v[..PAYLOAD_SIZE])?;
        let mut payload = Payload::deserialize(&mut &v[..PAYLOAD_SIZE])?;
        v.clear();
        worker.work(payload.work_iterations, payload.randomness);
        payload.randomness = shenango::rdtsc();
        payload.serialize_into(&mut v)?;
        Ok(socket.write_all(&v[..])?)
    };
    loop {
        if let Err(e) = r() as io::Result<()> {
            match e.raw_os_error() {
                Some(-104) | Some(104) => break,
                _ => {}
            }
            if e.kind() != ErrorKind::UnexpectedEof {
                println!("Receive thread: {}", e);
            }
            break;
        }
    }
}

fn run_tcp_server(backend: Backend, addr: SocketAddrV4, worker: Arc<FakeWorker>) {
    let tcpq = backend.create_tcp_listener(addr).unwrap();
    println!("Bound to address {}", addr);
    loop {
        match tcpq.accept() {
            Ok(mut c) => {
                let worker = worker.clone();
                backend.spawn_thread(move || socket_worker(&mut c, worker));
            }
            Err(e) => {
                println!("Listener: {}", e);
            }
        }
    }
}

fn run_spawner_server(addr: SocketAddrV4, workerspec: &str) {
    static mut SPAWNER_WORKER: Option<FakeWorker> = None;
    unsafe {
        SPAWNER_WORKER = Some(FakeWorker::create(workerspec).unwrap());
    }
    extern "C" fn echo(d: *mut shenango::ffi::udp_spawn_data) {
        unsafe {
            let buf = slice::from_raw_parts((*d).buf as *mut u8, (*d).len as usize);
            let mut payload = Payload::deserialize(&mut &buf[..]).unwrap();
            let worker = SPAWNER_WORKER.as_ref().unwrap();
            worker.work(payload.work_iterations, payload.randomness);
            payload.randomness = shenango::rdtsc();
            let mut array = ArrayVec::<_, PAYLOAD_SIZE>::new();
            payload.serialize_into(&mut array).unwrap();
            let _ = UdpSpawner::reply(d, array.as_slice());
            UdpSpawner::release_data(d);
        }
    }

    let _s = unsafe { UdpSpawner::new(addr, echo).unwrap() };

    let wg = shenango::WaitGroup::new();
    wg.add(1);
    wg.wait();
}

fn run_memcached_preload(
    proto: MemcachedProtocol,
    backend: Backend,
    tport: Transport,
    addr: SocketAddrV4,
    nthreads: usize,
) -> bool {
    let perthread = (proto.nvalues as usize + nthreads - 1) / nthreads;
    let join_handles: Vec<JoinHandle<_>> = (0..nthreads)
        .map(|i| {
            let proto = proto.clone();
            backend.spawn_thread(move || {
                let sock1 = Arc::new(match tport {
                    Transport::Tcp => backend.create_tcp_connection(None, addr).unwrap(),
                    Transport::Udp => backend
                        .create_udp_connection("0.0.0.0:0".parse().unwrap(), Some(addr))
                        .unwrap(),
                });
                let socket = sock1.clone();
                backend.spawn_thread(move || {
                    backend.sleep(Duration::from_secs(520));
                    if Arc::strong_count(&socket) > 1 {
                        println!("Timing out socket");
                        socket.shutdown();
                    }
                });

                let mut vec_s: Vec<u8> = Vec::with_capacity(4096);
                let mut vec_r: Vec<u8> = vec![0; 4096];
                let mut buf = Buffer::new(&mut vec_r[..]);
                for n in 0..perthread {
                    vec_s.clear();
                    proto.set_request((i * perthread + n) as u64, 0, &mut vec_s);

                    if let Err(e) = (&*sock1).write_all(&vec_s[..]) {
                        println!("Preload send ({}/{}): {}", n, perthread, e);
                        return false;
                    }

                    if let Err(e) = proto.read_response(&sock1, &mut buf) {
                        println!("preload receive ({}/{}): {}", n, perthread, e);
                        return false;
                    }
                }
                true
            })
        })
        .collect();

    return join_handles.into_iter().all(|j| j.join().unwrap());
}

#[derive(Copy, Clone)]
struct RequestSchedule {
    arrival: Distribution,
    service: Distribution,
    output: OutputMode,
    runtime: Duration,
    rps: usize,
    discard_pct: f32,
}

#[derive(Clone)]
struct TraceResult {
    actual_start: Option<Duration>,
    target_start: Duration,
    completion_time: Option<Duration>,
    server_tsc: u64,
    server_port: Option<u16>,
    queue_len: Option<u32>,
}

impl PartialOrd for TraceResult {
    fn partial_cmp(&self, other: &TraceResult) -> Option<std::cmp::Ordering> {
        self.server_tsc.partial_cmp(&other.server_tsc)
    }
}

impl PartialEq for TraceResult {
    fn eq(&self, other: &TraceResult) -> bool {
        self.server_tsc == other.server_tsc
    }
}

#[derive(Clone)]
struct ScheduleResult {
    packet_count: usize,
    drop_count: usize,
    never_sent_count: usize,
    first_send: Option<Duration>,
    last_send: Option<Duration>,
    last_recv: Option<Duration>,
    latencies: BTreeMap<u64, usize>,
    latencies_raw: Vec<u64>,
    first_tsc: Option<u64>,
    trace: Option<Vec<TraceResult>>,
}

fn gen_classic_packet_schedule(
    runtime: Duration,
    packets_per_second: usize,
    output: OutputMode,
    distribution: Distribution,
    ramp_up_seconds: usize,
    nthreads: usize,
    discard_pct: f32,
) -> Vec<RequestSchedule> {
    let mut sched: Vec<RequestSchedule> = Vec::new();

    /* Ramp up in 100ms increments */
    for t in 1..(10 * ramp_up_seconds) {
        let rate = t * packets_per_second / (ramp_up_seconds * 10);

        if rate == 0 {
            continue;
        }

        // println!("{} pps : {} ns", rate, nthreads * 1000_000_000 / rate);

        sched.push(RequestSchedule {
            // arrival: Distribution::Constant((nthreads * 1000_000_000 / rate) as u64),
            arrival: Distribution::Exponential((nthreads * 1000_000_000 / rate) as f64),
            service: distribution,
            output: OutputMode::Silent,
            runtime: Duration::from_millis(100),
            rps: rate,
            discard_pct: 0.0,
        });
    }

    let ns_per_packet = nthreads * 1000_000_000 / packets_per_second;

    // println!("{} pps : {} ns", packets_per_second, ns_per_packet);

    sched.push(RequestSchedule {
        // arrival: Distribution::Constant(ns_per_packet as u64),
        arrival: Distribution::Exponential(ns_per_packet as f64),
        service: distribution,
        output: output,
        runtime: runtime,
        rps: packets_per_second,
        discard_pct: discard_pct,
    });

    sched
}

fn gen_loadshift_experiment(
    spec: &str,
    service: Distribution,
    nthreads: usize,
    output: OutputMode,
) -> Vec<RequestSchedule> {
    spec.split(",")
        .map(|step_spec| {
            let s: Vec<&str> = step_spec.split(":").collect();
            assert!(s.len() >= 2 && s.len() <= 3);
            let packets_per_second: u64 = s[0].parse().unwrap();
            let ns_per_packet = nthreads as u64 * 1000_000_000 / packets_per_second;
            let micros = s[1].parse().unwrap();
            let output = match s.len() {
                2 => output,
                3 => OutputMode::Silent,
                _ => unreachable!(),
            };
            RequestSchedule {
                // arrival: Distribution::Constant(ns_per_packet as u64),
                arrival: Distribution::Exponential(ns_per_packet as f64),
                service: service,
                output: output,
                runtime: Duration::from_micros(micros),
                rps: packets_per_second as usize,
                discard_pct: 0.0,
            }
        })
        .collect()
}

fn process_result_final(
    sched: &RequestSchedule,
    results: Vec<ScheduleResult>,
    wct_start: SystemTime,
    sched_start: Duration,
) -> bool {
    let mut buckets: BTreeMap<u64, usize> = BTreeMap::new();
    let mut latencies_raw: Vec<u64> = Vec::new();

    let packet_count = results.iter().map(|res| res.packet_count).sum::<usize>();
    let drop_count = results.iter().map(|res| res.drop_count).sum::<usize>();
    let never_sent_count = results
        .iter()
        .map(|res| res.never_sent_count)
        .sum::<usize>();
    let first_send = results.iter().filter_map(|res| res.first_send).min();
    let last_send = results.iter().filter_map(|res| res.last_send).max();
    let last_recv = results.iter().filter_map(|res| res.last_recv).max();
    let first_tsc = results.iter().filter_map(|res| res.first_tsc).min();
    let start_unix = wct_start + sched_start;

    if let OutputMode::Silent = sched.output {
        return true;
    }

    if packet_count <= 1 {
        println!("WARNING: packet_count <= 1");
        println!(
            "[RESULT] {}, {}, 0, {}, {}, {}",
            sched.arrival.name(),
            sched.rps,
            drop_count,
            never_sent_count,
            start_unix.duration_since(UNIX_EPOCH).unwrap().as_secs()
        );
        return false;
    }

    results.iter().for_each(|res| {
        for (k, v) in &res.latencies {
            *buckets.entry(*k).or_insert(0) += v;
        }
        for lat in &res.latencies_raw {
            latencies_raw.push(*lat);
        }
    });
    eprintln!("Total # latencies: {}", latencies_raw.len());

    let percentile = |p| {
        let idx = ((packet_count + drop_count) as f32 * p / 100.0) as usize;
        if idx >= packet_count {
            return INFINITY;
        }

        let mut seen = 0;
        for k in buckets.keys() {
            seen += buckets[k];
            if seen >= idx {
                return *k as f32;
            }
        }
        return INFINITY;
    };

    if let OutputMode::Live = sched.output {
        println!(
            "RPS: {}\tMedian (us): {: <7}\t99th (us): {: <7}\t99.9th (us): {: <7}",
            sched.rps,
            percentile(50.0) as usize,
            percentile(99.0) as usize,
            percentile(99.9) as usize
        );
        return true;
    }

    let first_send = first_send.unwrap();
    let start_unix = wct_start + first_send;

    println!(
        "[RESULT] {}, {}, {}, {}, {}, {}, {:.1}, {:.1}, {:.1}, {:.1}, {:.1}, {}, {}",
        sched.arrival.name(),
        sched.rps,
        (packet_count + drop_count) as u64 * 1000_000_000
            / duration_to_ns(last_send.unwrap() - first_send),
        packet_count as u64 * 1000_000_000 / duration_to_ns(last_recv.unwrap() - first_send),
        drop_count,
        never_sent_count,
        percentile(50.0),
        percentile(90.0),
        percentile(99.0),
        percentile(99.9),
        percentile(99.99),
        start_unix.duration_since(UNIX_EPOCH).unwrap().as_secs(),
        first_tsc.unwrap()
    );

    
    unsafe {
        if let Some(exptid) = &EXPTID {
            if exptid != "null" {
                if let Ok(mut file) = File::create(format!("{}.latency", exptid)) {
                    let mut latencies: Vec<f64> = Vec::new();
                    let mut counts: Vec<usize> = Vec::new();

                    for (k, v) in buckets.iter() {
                        latencies.push(*k as f64);
                        counts.push(*v);
                    }

                    let total_count: u32 = counts.iter().map(|&count| count as u32).sum();
                    
                    
                    write!(file, "Latencies: \n").expect("Failed to write to file");
                    let mut cumulative_percentage = 0.0;
                    for (k, v) in buckets.iter() {
                        let percentage = (*v as f64 / total_count as f64) * 100.0; // Calculate the percentage
                        cumulative_percentage += percentage;
                        write!(file, "{},{},{:.2}\n", k, buckets[k], cumulative_percentage).expect("Failed to write to file");
                    }
                    writeln!(file, "").expect("Failed to write to file");
                    
                    
                    let mean: f64 = latencies.iter().zip(counts.iter()).map(|(&l, &c)| l * c as f64).sum::<f64>() / total_count as f64;
                    let squared_diffs: Vec<f64> = latencies.iter().map(|&x| (x - mean).powi(2)).collect();
                    let variance: f64 = squared_diffs.iter().sum::<f64>() / total_count as f64;
                    let standard_deviation: f64 = variance.sqrt();
                    // let sqdiff = squared_diffs.iter().sum::<f64>();
                    // writeln!(file, "Total Count: {}", total_count).expect("Failed to write to file");
                    writeln!(file, "Mean: {:.2}", mean).expect("Failed to write to file");
                    // writeln!(file, "squared_diffs: {:.2}", sqdiff).expect("Failed to write to file");
                    // writeln!(file, "variance: {:.2}", variance).expect("Failed to write to file");
                    writeln!(file, "Standard Deviation: {:.2}", standard_deviation).expect("Failed to write to file");

                } else {
                    eprintln!("Failed to create file {}.latency", exptid);
                }

                if let Ok(mut file) = File::create(format!("{}.latency_raw", exptid)) {
                    for (index, latency) in latencies_raw.iter().enumerate() {
                        writeln!(file, "{},{}", index, latency)
                            .expect("Failed to write to file");
                    }
                } else {
                    eprintln!("Failed to create file {}..latency_raw", exptid);
                }
            }
        }
    }


    if let OutputMode::Trace = sched.output {
        if let Some(exptid) = unsafe {&EXPTID} {
            if exptid != "null" {
                
                let lat_file_path = format!("{}.latency_trace", exptid);
                let mut lat_file = OpenOptions::new()
                                                .append(true)
                                                .create(true)
                                                .open(&lat_file_path)
                                                .expect("Failed to open file");
                
                // UNCOMMENT FOR RECV QUEUE LENGTH EVAL.
                // let recvq_file_path = format!("{}.recv_qlen", exptid);
                // let mut recvq_file = OpenOptions::new()
                //                                 .append(true)
                //                                 .create(true)
                //                                 .open(&recvq_file_path)
                //                                 .expect("Failed to open file");
                // UNCOMMENT FOR RECV QUEUE LENGTH EVAL.


                for p in results.into_iter().filter_map(|p| p.trace).kmerge() {
                    
                    if let Some(completion_time) = p.completion_time {
                        let target_start = duration_to_ns(p.target_start);
                        let lat_in_us = duration_to_ns(completion_time - p.actual_start.unwrap()) as u64 / 1000;
                        // unsafe{ LATENCY_TRACE_RESULTS.push((duration_to_ns(actual_start), lat)) };
                        writeln!(lat_file, "{},{}", target_start, lat_in_us).expect("Failed to write to lat_file");
                        
                        
                        // UNCOMMENT FOR RECV QUEUE LENGTH EVAL.
                        // let tsc = p.server_tsc;
                        // let server_port = p.server_port.unwrap();
                        // let recv_qlen = p.queue_len.unwrap();
                        
                        // writeln!(recvq_file, "{},{},{},{},{}", target_start, lat_in_us, recv_qlen, server_port, tsc).expect("Failed to write to recvq_file");
                        // UNCOMMENT FOR RECV QUEUE LENGTH EVAL.
                    }
                }
            }
        }
    }

    true
}

fn process_result(sched: &RequestSchedule, packets: &mut [Packet]) -> Option<ScheduleResult> {
    // println!("packets.len(): {}", packets.len());
    if packets.len() == 0 {
        return None;
    }

    // Discard the first X% of the packets.
    let pidx = packets.len() as f32 * sched.discard_pct / 100.0;
    let packets = &mut packets[pidx as usize..];

    let mut never_sent = 0;
    let mut dropped = 0;
    let mut latencies = BTreeMap::new();
    let mut latencies_raw: Vec<u64> = Vec::new(); 
    for p in packets.iter() {
        match (p.actual_start, p.completion_time) {
            (None, _) => {
                // println!("never sent!");
                never_sent += 1
            },
            (_, None) => {
                // println!("dropped!");
                dropped += 1
            },
            (Some(ref start), Some(ref end)) => {
                // println!("success!");
                let latency_ns = duration_to_ns(*end - *start);
                // Add the latency to the latencies_raw vector
                latencies_raw.push(latency_ns / 1000);
                
                // Update the latencies BTreeMap
                *latencies.entry(latency_ns / 1000).or_insert(0) += 1;
            }
        }
    }
    // println!("packets.len(): {}, dropped: {}, never_sent: {}", packets.len(), dropped, never_sent);
    if packets.len() - dropped - never_sent <= 1 {
        return None;
    }

    if let OutputMode::Silent = sched.output {
        return None;
    }

    let trace = match sched.output {
        OutputMode::Trace => {
            let mut traceresults: Vec<_> = packets
                .into_iter()
                .filter_map(|p| {
                    if !p.completion_time.is_some() {
                        return None;
                    }

                    Some(TraceResult {
                        actual_start: p.actual_start,
                        target_start: p.target_start,
                        completion_time: p.completion_time,
                        server_tsc: p.completion_server_tsc.unwrap(),
                        server_port: p.server_port,
                        queue_len: p.queue_len,
                    })
                })
                .collect();
            traceresults.sort_by_key(|p| p.server_tsc);
            Some(traceresults)
        }
        _ => None,
    };
    
    Some(ScheduleResult {
        packet_count: packets.len() - dropped - never_sent,
        drop_count: dropped,
        never_sent_count: never_sent,
        first_send: packets.iter().filter_map(|p| p.actual_start).min(),
        last_send: packets.iter().filter_map(|p| p.actual_start).max(),
        last_recv: packets.iter().filter_map(|p| p.completion_time).max(),
        latencies: latencies,
        latencies_raw: latencies_raw,
        first_tsc: packets.iter().filter_map(|p| p.completion_server_tsc).min(),
        trace: trace,
    })
}

fn lognormal_interarrival(rand_gaussian: f64) -> f64 {
    lognormal(6.2726064589176, 1.8779582422664103, rand_gaussian)/10.0
}

fn lognormal_on(rand_gaussian: f64) -> f64 {
    lognormal(2.868183746231741, 0.4301567266932548, rand_gaussian)
}

fn lognormal_off(rand_gaussian: f64) -> f64 {
    lognormal(1.0872546045494296, 0.38168878514903204, rand_gaussian)
}
/* unit: microseconds */

fn lognormal(mu: f64, sigma: f64, rand_gaussian: f64) -> f64 {
    f64::exp(mu + sigma * rand_gaussian)
}

fn gen_packets_for_schedule(schedules: &Arc<Vec<RequestSchedule>>) -> (Vec<Packet>, Vec<usize>) {
    use rand_distr::Distribution;
    let mut packets: Vec<Packet> = Vec::new();
    
    // let mut rng: Mt64 = Mt64::new(rand::thread_rng().gen::<u64>());
    let mut rng: Mt64 = Mt64::new(292383402);
    
    let mut sched_boundaries = Vec::new();
    let mut last = 100_000_000; //as u64 + rng.gen::<u64>() % 5;
    let mut end = 100_000_000;
    
    if let Some(onoff) = unsafe{ &ONOFF } {
        if onoff == "1" {
            // eprintln!("Running with On/Off pattern");
            // let seed = rng.gen();
            let seed = 292383402;
            let mut rand_gaussian = 0.0; // Replace with your random Gaussian value
            let mut important_rand: StdRng = StdRng::seed_from_u64(seed);

            // Define the mean (mu) and standard deviation (sigma) of the Gaussian distribution
            let mu = 0.0;
            let sigma = 1.0;
            let normal = Normal::new(mu, sigma).unwrap();    
            
            for sched in schedules.iter() {
                end += duration_to_ns(sched.runtime);

                let mut onoff_intervals: Vec<(bool, (u64, u64))> = Vec::new();
                let mut is_on = false;
                let mut onoff_timestamp = last;
                let mut total_on_time: u64 = 0;
                loop{
                    rand_gaussian = normal.sample(&mut important_rand);
                    let interval_in_ns = match is_on {
                        true => std::cmp::min( (lognormal_on(rand_gaussian) * 1000000.0) as u64, end - onoff_timestamp ),
                        false => std::cmp::min( (lognormal_on(rand_gaussian) * 1000000.0) as u64, end - onoff_timestamp ),
                    };
                    
                    onoff_intervals.push((is_on, (onoff_timestamp, onoff_timestamp + interval_in_ns)));
                    onoff_timestamp = onoff_timestamp + interval_in_ns;

                    if is_on {
                        total_on_time = total_on_time + interval_in_ns;
                    }
                    if onoff_timestamp == end {
                        break;
                    }
                    is_on = !is_on;
                }
                let mut ontime_weight = total_on_time as f64 / duration_to_ns(sched.runtime) as f64;
                // println!("total_on_time: {}", total_on_time);
                
                // onoff_intervals = vec![
                //     (true, (100_000_000, 2_100_000_000)),
                //     (false, (2_100_000_000, 7_100_000_000)),
                //     (true, (7_100_000_000, 10_100_000_000)),
                //     // Add more pairs as needed
                // ];
                // ontime_weight = 1.0/2.0;
                for (is_on, (_, interval_end)) in &onoff_intervals {
                    loop {
                        if last >= *interval_end {
                            break;
                        }
                        if *is_on {
                            packets.push(Packet {
                                randomness: rng.gen::<u64>(),
                                target_start: Duration::from_nanos(last),
                                work_iterations: sched.service.sample(&mut rng),
                                ..Default::default()
                            });
                        }

                        let nxt = last + sched.arrival.onoff_sample(&mut rng, ontime_weight);
                        last = nxt;
                    }
                    
                }
                sched_boundaries.push(packets.len());
            }
        }
        else{
            // eprintln!("Running w/o On/Off pattern");
            for sched in schedules.iter() {
                end += duration_to_ns(sched.runtime);
                
                loop {
                    packets.push(Packet {
                        randomness: rng.gen::<u64>(),
                        target_start: Duration::from_nanos(last),
                        work_iterations: sched.service.sample(&mut rng),
                        ..Default::default()
                    });
        
                    let nxt = last + sched.arrival.sample(&mut rng);
                    if nxt >= end {
                        last = nxt;
                        break;
                    }
                    last = nxt;
                }
                sched_boundaries.push(packets.len());
            }
        }
    }
    (packets, sched_boundaries)
}

fn run_client_worker(
    proto: Arc<Box<dyn LoadgenProtocol>>,
    backend: Backend,
    addr: SocketAddrV4,
    tport: Transport,
    wg: shenango::WaitGroup,
    wg_start: shenango::WaitGroup,
    schedules: Arc<Vec<RequestSchedule>>,
    index: usize,
    live_mode_socket: Option<Arc<Connection>>,
) -> Vec<Option<ScheduleResult>> {
    let mut payload = Vec::with_capacity(4096);
    let (mut packets, sched_boundaries) = gen_packets_for_schedule(&schedules);
    let src_addr = SocketAddrV4::new(Ipv4Addr::new(0, 0, 0, 0), (100 + index) as u16);
    let live_mode = live_mode_socket.is_some();
    let socket = match live_mode_socket {
        Some(sock) => sock,
        _ => Arc::new(match tport {
            Transport::Tcp => backend.create_tcp_connection(Some(src_addr), addr).unwrap(),
            Transport::Udp => backend.create_udp_connection(src_addr, Some(addr)).unwrap(),
        }),
    };

    let packets_per_thread = packets.len();
    let socket2 = socket.clone();
    let rproto = proto.clone();
    let wg2 = wg.clone();
    let received_packets = Arc::new(AtomicUsize::new(0));
    let received_packets2 = received_packets.clone();
    
    let receive_thread = backend.spawn_thread(move || {
        let mut recv_buf = vec![0; 4096];
        let mut receive_times: Vec<Option<(Instant, u64, u16, u32)>> = vec![None; packets_per_thread];
        let mut buf = Buffer::new(&mut recv_buf);
        let use_ordering = rproto.uses_ordered_requests();
        wg2.done();
        let mut recv_cnt = 0;
        for i in 0..receive_times.len() {
            // eprintln!("{} {}", i, receive_times.len());
            match rproto.read_response(&socket2, &mut buf) {
                Ok((mut idx, tsc)) => {
                    let port = (idx >> 32) as u16;
                    let queue_len = (idx & 0xffff_ffff) as u32;
                    if use_ordering {
                        idx = i;
                    }
                    // eprintln!("receive");
                    receive_times[idx] = Some((Instant::now(), tsc, port, queue_len));
                    recv_cnt += 1;
                    received_packets2.store(recv_cnt, Ordering::SeqCst)
                }
                Err(e) => {
                    match e.raw_os_error() {
                        Some(-103) | Some(-104) => break,
                        _ => (),
                    }
                    if e.kind() != ErrorKind::UnexpectedEof {
                        println!("Receive thread: {}", e);
                    }
                    break;
                }
            }
        }
        // eprintln!("Returning");
        receive_times
    });

    // Start a timer thread that cancels the send thread if it is still running
    // 500ms after it should have finished by triggering a shutdown on the socket.
    let last = packets[packets.len() - 1].target_start;
    let socket2 = socket.clone();
    let wg2 = wg.clone();
    let wg3 = wg_start.clone();
    let live_mode2 = live_mode;
    let mut nsent = 0;
    let timer = backend.spawn_thread(move || {
        wg2.done();
        wg3.wait();
        if live_mode2 {
            return;
        }
        backend.sleep(last + Duration::from_millis(500));
        if Arc::strong_count(&socket2) > 1 {
            socket2.shutdown();
        }
    });

    wg.done();
    wg_start.wait();
    let start = Instant::now();

    
    let pl = packets.len();
    for (i, packet) in packets.iter_mut().enumerate() {
        payload.clear();
        proto.gen_req(i, packet, &mut payload);

        let mut t = start.elapsed();
        while t + Duration::from_micros(1) < packet.target_start {
            backend.sleep(packet.target_start - t);
            t = start.elapsed();
        }
        if !live_mode && t > packet.target_start + Duration::from_micros(5) {
            continue;
        }

        // packet.actual_start = Some(packet.target_start);
        packet.actual_start = Some(start.elapsed());
        if let Err(e) = (&*socket).write_all(&payload[..]) {
            packet.actual_start = None;
            match e.raw_os_error() {
                Some(-32) | Some(-103) | Some(-104) => {}
                _ => println!("Send thread ({}/{}): {}", i, packets.len(), e),
            }
            break;
        }
        nsent += 1; 
        // println!("total: {}, nsent: {}", pl, nsent);
        // Check if the counter has reached 100
        // if nsent >= 1000 {
        //     break;  // Break the loop after 100 iterations
        // }
    }

    // wait for the timer thread to end
    timer.join().unwrap();
    if let Transport::Tcp = tport {
        let mut prev_received = received_packets.load(Ordering::SeqCst);

        while received_packets.load(Ordering::SeqCst) < nsent {
            backend.sleep(Duration::from_secs(1));

            let current_received = received_packets.load(Ordering::SeqCst);

            // Check if the count has increased, otherwise break the loop
            if current_received == prev_received {
                break;
            }

            prev_received = current_received;
        }
    }

    socket.shutdown();

    receive_thread
        .join()
        .unwrap()
        .into_iter()
        .zip(
            packets
                .iter_mut()
                .filter(|p| !proto.uses_ordered_requests() || p.actual_start.is_some()),
        )
        .for_each(|(c, p)| {
            if let Some((inst, tsc, port, queue_len)) = c {
                (*p).completion_time = Some(inst - start);
                (*p).completion_server_tsc = Some(tsc);
                (*p).server_port = Some(port);
                (*p).queue_len = Some(queue_len);
            }
        });

    let mut start_index = 0;
    schedules
        .iter()
        .zip(sched_boundaries)
        .map(|(sched, end)| {
            let res = process_result(&sched, &mut packets[start_index..end]);
            start_index = end;
            res
        })
        .collect::<Vec<Option<ScheduleResult>>>()
}

fn run_live_client(
    proto: Arc<Box<dyn LoadgenProtocol>>,
    backend: Backend,
    addrs: &Vec<SocketAddrV4>,
    nthreads: usize,
    tport: Transport,
    barrier_group: &mut Option<lockstep::Group>,
    schedules: Vec<RequestSchedule>,
) {
    let schedules = Arc::new(schedules);

    let sockets: Vec<_> = (0..nthreads)
        .into_iter()
        .map(|i| {
            let addr = addrs[i % addrs.len()];
            let client_idx = 200 + i;
            let src_addr = SocketAddrV4::new(Ipv4Addr::new(0, 0, 0, 0), client_idx as u16);
            Arc::new(match tport {
                Transport::Tcp => backend.create_tcp_connection(Some(src_addr), addr).unwrap(),
                Transport::Udp => backend.create_udp_connection(src_addr, Some(addr)).unwrap(),
            })
        })
        .collect();

    let wg = shenango::WaitGroup::new();
    let wg_start = shenango::WaitGroup::new();

    loop {
        wg.add(3 * nthreads as i32);
        wg_start.add(1 as i32);
        let socks = sockets.clone();

        let conn_threads: Vec<_> = (0..nthreads)
            .into_iter()
            .zip(socks)
            .map(|(i, sock)| {
                let client_idx = 100 + i;
                let proto = proto.clone();
                let wg = wg.clone();
                let wg_start = wg_start.clone();
                let schedules = schedules.clone();
                let addr = addrs[i % addrs.len()];

                backend.spawn_thread(move || {
                    run_client_worker(
                        proto,
                        backend,
                        addr,
                        tport,
                        wg,
                        wg_start,
                        schedules,
                        client_idx,
                        Some(sock.clone()),
                    )
                })
            })
            .collect();

        wg.wait();

        if let Some(ref mut g) = *barrier_group {
            g.barrier();
        }

        wg_start.done();
        let start_unix = SystemTime::now();

        let mut packets: Vec<Vec<Option<ScheduleResult>>> = conn_threads
            .into_iter()
            .map(|s| s.join().unwrap())
            .collect();

        let mut sched_start = Duration::from_nanos(100_000_000);
        schedules
            .iter()
            .enumerate()
            .map(|(i, sched)| {
                let perthread = packets.iter_mut().filter_map(|p| p[i].take()).collect();
                let r = process_result_final(sched, perthread, start_unix, sched_start);
                sched_start += sched.runtime;
                r
            })
            .collect::<Vec<bool>>()
            .into_iter()
            .all(|p| p);
    }
}

fn run_client(
    proto: Arc<Box<dyn LoadgenProtocol>>,
    backend: Backend,
    addrs: &Vec<SocketAddrV4>,
    nthreads: usize,
    tport: Transport,
    barrier_group: &mut Option<lockstep::Group>,
    schedules: Vec<RequestSchedule>,
    index: usize,
) -> bool {
    let schedules = Arc::new(schedules);
    let wg = shenango::WaitGroup::new();

    wg.add(3 * nthreads as i32);
    let wg_start = shenango::WaitGroup::new();
    wg_start.add(1 as i32);

    let conn_threads: Vec<_> = (0..nthreads)
        .into_iter()
        .map(|i| {
            let client_idx = 100 + (index * nthreads) + i;
            let proto = proto.clone();
            let wg = wg.clone();
            let wg_start = wg_start.clone();
            let schedules = schedules.clone();
            let addr = addrs[i % addrs.len()];

            backend.spawn_thread(move || {
                run_client_worker(
                    proto, backend, addr, tport, wg, wg_start, schedules, client_idx, None,
                )
            })
        })
        .collect();

    backend.sleep(Duration::from_secs(1));
    wg.wait();

    if let Some(ref mut g) = *barrier_group {
        g.barrier();
    }

    wg_start.done();
    let start_unix = SystemTime::now();

    let mut packets: Vec<Vec<Option<ScheduleResult>>> = conn_threads
        .into_iter()
        .map(|s| s.join().unwrap())
        .collect();

    let mut sched_start = Duration::from_nanos(100_000_000);
    schedules
        .iter()
        .enumerate()
        .map(|(i, sched)| {
            let perthread = packets.iter_mut().filter_map(|p| p[i].take()).collect();
            let r = process_result_final(sched, perthread, start_unix, sched_start);
            sched_start += sched.runtime;
            r
        })
        .collect::<Vec<bool>>()
        .into_iter()
        .all(|p| p)
}

fn run_local(
    backend: Backend,
    nthreads: usize,
    worker: Arc<FakeWorker>,
    schedules: Vec<RequestSchedule>,
) -> bool {
    let schedules = Arc::new(schedules);

    let packet_schedules: Vec<Vec<Packet>> = (0..nthreads)
        .map(|_| gen_packets_for_schedule(&schedules).0)
        .collect();

    let start_unix = SystemTime::now();
    let start = Instant::now();

    struct AtomicU64Pointer(*const AtomicU64);
    unsafe impl Send for AtomicU64Pointer {}

    let mut send_threads = Vec::new();
    for mut packets in packet_schedules {
        let worker = worker.clone();
        let schedules = schedules.clone();
        send_threads.push(backend.spawn_thread(move || {
            let remaining = Arc::new(AtomicUsize::new(packets.len()));
            for i in 0..packets.len() {
                let (work_iterations, completion_time_ns, rnd) = {
                    let packet = &mut packets[i];

                    let mut t = start.elapsed();
                    while t < packet.target_start {
                        t = start.elapsed();
                    }

                    packet.actual_start = Some(start.elapsed());
                    (
                        packet.work_iterations,
                        AtomicU64Pointer(&packet.completion_time_ns as *const AtomicU64),
                        packet.randomness,
                    )
                };

                let remaining = remaining.clone();
                let worker = worker.clone();
                backend.spawn_thread(move || {
                    worker.work(work_iterations, rnd);
                    unsafe {
                        (*completion_time_ns.0)
                            .store(start.elapsed().as_nanos() as u64, Ordering::SeqCst);
                    }
                    remaining.fetch_sub(1, Ordering::SeqCst);
                });
            }

            while remaining.load(Ordering::SeqCst) > 0 {
                // do nothing
            }

            for p in packets.iter_mut() {
                p.completion_time = Some(Duration::from_nanos(
                    p.completion_time_ns.load(Ordering::SeqCst),
                ));
            }

            let mut start = Duration::from_nanos(100_000_000);
            let mut start_index = 0;

            schedules
                .iter()
                .map(|sched| {
                    let npackets = packets[start_index..]
                        .iter()
                        .position(|p| p.target_start >= start + sched.runtime)
                        .unwrap_or(packets.len() - start_index - 1)
                        + 1;
                    let res =
                        process_result(&sched, &mut packets[start_index..start_index + npackets]);
                    start = packets[start_index + npackets - 1].target_start;
                    start_index += npackets;
                    res
                })
                .collect::<Vec<Option<ScheduleResult>>>()
        }))
    }

    let mut packets: Vec<Vec<Option<ScheduleResult>>> = send_threads
        .into_iter()
        .map(|s| s.join().unwrap())
        .collect();

    let mut sched_start = Duration::from_nanos(100_000_000);
    schedules
        .iter()
        .enumerate()
        .map(|(i, sched)| {
            let perthread = packets.iter_mut().filter_map(|p| p[i].take()).collect();
            let r = process_result_final(sched, perthread, start_unix, sched_start);
            sched_start += sched.runtime;
            r
        })
        .collect::<Vec<bool>>()
        .into_iter()
        .all(|p| p)
}

fn get_zipf_distribution(
    total_pps: usize,
    alpha: f64,
    nthreads: usize
) -> impl Iterator<Item = f64> {
    /*
        zipf = [0] * n
        c = 0.0
        for i in range(n):
            c = c + (1.0 / (i+1)**alpha)
        c = 1 / c
        for i in range(n):
            zipf[i] = (c / (i+1)**alpha)
        return zipf
    */

    let c = (1..nthreads + 1)
        .fold(0.0, |acc, i| acc + (i as f64).powf(alpha).recip())
        .recip();

    (1..nthreads + 1).map(move |i| (total_pps as f64 * (c / (i as f64).powf(alpha))))
}

fn zipf_gen_classic_packet_schedule(
    runtime: Duration,
    pps: f64,
    output: OutputMode,
    distribution: Distribution,
    ramp_up_seconds: usize,
    discard_pct: f32,
) -> Vec<RequestSchedule> {
    let mut sched: Vec<RequestSchedule> = Vec::new();

    /* Ramp up in 100ms increments */
    for t in 1..(10 * ramp_up_seconds) {
        let rate = t as f64 * pps / (ramp_up_seconds as f64 * 10.0);

        if rate as usize / 1000 == 0 {
            continue;
        }

        let ns_per_packet = 1_000_000_000.0 / rate;

        // eprintln!("rampup {} pps : {} ns", rate as usize, ns_per_packet as usize);

        sched.push(RequestSchedule {
            // arrival: Distribution::Constant(ns_per_packet as u64),
            arrival: Distribution::Exponential(ns_per_packet as f64),
            service: distribution,
            output: OutputMode::Silent,
            runtime: Duration::from_millis(100),
            rps: rate as usize,
            discard_pct: 0.0,
        });
    }

    let ns_per_packet = 1_000_000_000.0 / pps;

    eprint!("{}    ", pps as usize);
    
    sched.push(RequestSchedule {
        // arrival: Distribution::Constant(ns_per_packet as u64),
        arrival: Distribution::Exponential(ns_per_packet as f64),
        service: distribution,
        output,
        runtime,
        rps: pps as usize,
        discard_pct,
    });

    sched
}

fn zipf_gen_loadshift_experiment<R: Rng>(
    spec: &str,
    service: Distribution,
    nthreads: usize,
    alpha: f64,
    output: OutputMode,
    rng: &mut R,
) -> (Vec<Vec<RequestSchedule>>, Vec<usize>) {
    spec.split(",")
        .fold(
            (vec![vec![]; nthreads], vec![]),
            |(mut acc, mut ppss), step_spec| {
                let s: Vec<&str> = step_spec.split(":").collect();
                assert!(s.len() >= 2 && s.len() <= 3);
                let packets_per_second: u64 = s[0].parse().unwrap();
                let micros = s[1].parse().unwrap();
                let output = match s.len() {
                    2 => output,
                    3 => OutputMode::Silent,
                    _ => unreachable!(),
                };
                // eprintln!("\n"); 
                ppss.push(packets_per_second as usize);
                get_zipf_distribution(packets_per_second as usize, alpha, nthreads)
                    .enumerate()
                    .for_each(|(i, pps)| {
                        let ns_per_packet = 1_000_000_000.0 / pps;
                        // eprint!("{}    ", pps as usize);
                        acc[i].push(
                            RequestSchedule {
                                // arrival: Distribution::Constant(ns_per_packet as u64),
                                arrival: Distribution::Exponential(ns_per_packet as f64),
                                service,
                                output,
                                runtime: Duration::from_micros(micros),
                                rps: pps as usize,
                                discard_pct: 0.0,
                            }
                        );
                    });
                acc.shuffle(rng);
                // for schedule_vec in &acc {
                //     for schedule in schedule_vec {
                //         eprint!("{}     ", schedule.rps);
                //     }
                // }eprint!("\n\n");
                (acc, ppss)
            }
        )
}

fn zipf_process_result_final(
    total_pps: usize,
    scheds: Vec<RequestSchedule>,
    results: Vec<ScheduleResult>,
    wct_start: SystemTime,
    sched_start: Duration,
) -> bool {
    let mut buckets: BTreeMap<u64, usize> = BTreeMap::new();
    let mut latencies_raw: Vec<u64> = Vec::new();

    let packet_count = results.iter().map(|res| res.packet_count).sum::<usize>();
    let drop_count = results.iter().map(|res| res.drop_count).sum::<usize>();
    let never_sent_count = results
        .iter()
        .map(|res| res.never_sent_count)
        .sum::<usize>();
    let first_send = results.iter().filter_map(|res| res.first_send).min();
    let last_send = results.iter().filter_map(|res| res.last_send).max();
    let last_recv = results.iter().filter_map(|res| res.last_recv).max();
    let first_tsc = results.iter().filter_map(|res| res.first_tsc).min();
    let start_unix = wct_start + sched_start;

    if let OutputMode::Silent = scheds[0].output {
        return true;
    }

    // let total_pps = scheds.iter().map(|e| e.rps).sum::<usize>();

    if packet_count <= 1 {
        println!(
            "\n\n[RESULT] {}, {}, 0, {}, {}, {}",
            scheds[0].arrival.name(),
            total_pps,
            drop_count,
            never_sent_count,
            start_unix.duration_since(UNIX_EPOCH).unwrap().as_secs()
        );
        return false;
    }

    results.iter().for_each(|res| {
        for (k, v) in &res.latencies {
            *buckets.entry(*k).or_insert(0) += v;
        }
        for lat in &res.latencies_raw {
            latencies_raw.push(*lat);
        }
    });

    let percentile = |p| {
        let idx = ((packet_count + drop_count) as f32 * p / 100.0) as usize;
        if idx >= packet_count {
            return INFINITY;
        }

        let mut seen = 0;
        for k in buckets.keys() {
            seen += buckets[k];
            if seen >= idx {
                return *k as f32;
            }
        }
        return INFINITY;
    };

    if let OutputMode::Live = scheds[0].output {
        println!(
            "RPS: {}\tMedian (us): {: <7}\t99th (us): {: <7}\t99.9th (us): {: <7}",
            total_pps,
            percentile(50.0) as usize,
            percentile(99.0) as usize,
            percentile(99.9) as usize
        );
        return true;
    }

    let first_send = first_send.unwrap();
    let start_unix = wct_start + first_send;

    let target = results.iter().map(|res| {
        (res.packet_count + res.drop_count) as u64 * 1000_000_000
            / duration_to_ns(res.last_send.unwrap() - res.first_send.unwrap())
    }).fold(0, |s, e| s + e);

    let actual = results.iter().map(|res| {
        res.packet_count as u64 * 1000_000_000 / duration_to_ns(res.last_recv.unwrap() - res.first_send.unwrap())
    }).fold(0, |s, e| s + e);

    println!(
        "\n\n[RESULT] {}, {}, {}, {}, {}, {}, {:.1}, {:.1}, {:.1}, {:.1}, {:.1}, {}, {}",
        scheds[0].arrival.name(),
        total_pps,
        target,
        actual,
        drop_count,
        never_sent_count,
        percentile(50.0),
        percentile(90.0),
        percentile(99.0),
        percentile(99.9),
        percentile(99.99),
        start_unix.duration_since(UNIX_EPOCH).unwrap().as_secs(),
        first_tsc.unwrap()
    );

    
    unsafe {
        if let Some(exptid) = &EXPTID {
            if exptid != "null" {
                if let Ok(mut file) = File::create(format!("{}.latency", exptid)) {
                    let mut latencies: Vec<f64> = Vec::new();
                    let mut counts: Vec<usize> = Vec::new();

                    for (k, v) in buckets.iter() {
                        latencies.push(*k as f64);
                        counts.push(*v);
                    }

                    let total_count: u32 = counts.iter().map(|&count| count as u32).sum();
                    
                    
                    write!(file, "Latencies: \n").expect("Failed to write to file");
                    let mut cumulative_percentage = 0.0;
                    for (k, v) in buckets.iter() {
                        let percentage = (*v as f64 / total_count as f64) * 100.0; // Calculate the percentage
                        cumulative_percentage += percentage;
                        write!(file, "{},{},{:.2}\n", k, buckets[k], cumulative_percentage).expect("Failed to write to file");
                    }
                    writeln!(file, "").expect("Failed to write to file");
                    
                    
                    let mean: f64 = latencies.iter().zip(counts.iter()).map(|(&l, &c)| l * c as f64).sum::<f64>() / total_count as f64;
                    let squared_diffs: Vec<f64> = latencies.iter().map(|&x| (x - mean).powi(2)).collect();
                    let variance: f64 = squared_diffs.iter().sum::<f64>() / total_count as f64;
                    let standard_deviation: f64 = variance.sqrt();
                    // let sqdiff = squared_diffs.iter().sum::<f64>();
                    // writeln!(file, "Total Count: {}", total_count).expect("Failed to write to file");
                    writeln!(file, "Mean: {:.2}", mean).expect("Failed to write to file");
                    // writeln!(file, "squared_diffs: {:.2}", sqdiff).expect("Failed to write to file");
                    // writeln!(file, "variance: {:.2}", variance).expect("Failed to write to file");
                    writeln!(file, "Standard Deviation: {:.2}", standard_deviation).expect("Failed to write to file");

                } else {
                    eprintln!("Failed to create file {}.latency", exptid);
                }

                if let Ok(mut file) = File::create(format!("{}.latency_raw", exptid)) {
                    for (index, latency) in latencies_raw.iter().enumerate() {
                        writeln!(file, "{},{}", index, latency)
                            .expect("Failed to write to file");
                    }
                } else {
                    eprintln!("Failed to create file {}..latency_raw", exptid);
                }
            }
        }
    }


    if let OutputMode::Trace = scheds[0].output {
        if let Some(exptid) = unsafe {&EXPTID} {
            if exptid != "null" {
                
                let lat_file_path = format!("{}.latency_trace", exptid);
                let mut lat_file = OpenOptions::new()
                                                .append(true)
                                                .create(true)
                                                .open(&lat_file_path)
                                                .expect("Failed to open file");
                
                // UNCOMMENT FOR RECV QUEUE LENGTH EVAL.
                // let recvq_file_path = format!("{}.recv_qlen", exptid);
                // let mut recvq_file = OpenOptions::new()
                //                                 .append(true)
                //                                 .create(true)
                //                                 .open(&recvq_file_path)
                //                                 .expect("Failed to open file");
                // UNCOMMENT FOR RECV QUEUE LENGTH EVAL.


                for p in results.into_iter().filter_map(|p| p.trace).kmerge() {
                    
                    if let Some(completion_time) = p.completion_time {
                        let target_start = duration_to_ns(p.target_start);
                        let lat_in_us = duration_to_ns(completion_time - p.actual_start.unwrap()) as u64 / 1000;
                        // unsafe{ LATENCY_TRACE_RESULTS.push((duration_to_ns(actual_start), lat)) };
                        writeln!(lat_file, "{},{}", target_start, lat_in_us).expect("Failed to write to lat_file");
                        
                        
                        // UNCOMMENT FOR RECV QUEUE LENGTH EVAL.
                        // let tsc = p.server_tsc;
                        // let server_port = p.server_port.unwrap();
                        // let recv_qlen = p.queue_len.unwrap();
                        
                        // writeln!(recvq_file, "{},{},{},{},{}", target_start, lat_in_us, recv_qlen, server_port, tsc).expect("Failed to write to recvq_file");
                        // UNCOMMENT FOR RECV QUEUE LENGTH EVAL.
                    }
                }
            }
        }
    }

    true
}

fn zipf_run_client(
    total_ppss: Vec<usize>,
    proto: Arc<Box<dyn LoadgenProtocol>>,
    backend: Backend,
    addrs: &Vec<SocketAddrV4>,
    nthreads: usize,
    tport: Transport,
    barrier_group: &mut Option<lockstep::Group>,
    schedules: Vec<Arc<Vec<RequestSchedule>>>,
    index: usize,
) -> bool {
    let wg = shenango::WaitGroup::new();

    wg.add(3 * nthreads as i32);
    let wg_start = shenango::WaitGroup::new();
    wg_start.add(1 as i32);

    let conn_threads: Vec<_> = schedules
        .iter()
        .enumerate()
        .map(|(i, schedules)| {
            let client_idx = 100 + (index * nthreads) + i;
            let proto = proto.clone();
            let wg = wg.clone();
            let wg_start = wg_start.clone();
            let schedules = schedules.clone();
            let addr = addrs[i % addrs.len()];

            backend.spawn_thread(move || {
                run_client_worker(
                    proto, backend, addr, tport, wg, wg_start, schedules, client_idx, None,
                )
            })
        })
        .collect();
    
    backend.sleep(Duration::from_secs(1));
    wg.wait();


    if let Some(ref mut g) = *barrier_group {
        g.barrier();
    }

    wg_start.done();
    let start_unix = SystemTime::now();

    let packets: Vec<Vec<Option<ScheduleResult>>> = conn_threads
        .into_iter()
        .map(|s| s.join().unwrap())
        .collect();

    let sched_start = Duration::from_nanos(100_000_000);

    let results = schedules.into_iter()
        .zip(packets.into_iter())
        .map(|(schedules, packets)| {
            let mut sched_start = sched_start;
            schedules.iter()
                .zip(packets.into_iter())
                .filter_map(|(sched, packet)| {
                    let old_sched_start = sched_start;
                    sched_start += sched.runtime;
                    if let OutputMode::Silent = sched.output { None }
                    else { Some((*sched, packet.expect("thread had zero results"), old_sched_start)) }
                })
                .collect_vec()
        })
        .collect_vec();

    /* let results = schedules.iter()
        .zip(packets.into_iter())
        .filter_map(|(schedules, packets)| {
            let last = packets.into_iter().last();
            if last.is_none() || last.as_ref().unwrap().is_none() { return None }
            let last = last.unwrap().unwrap();

            let sched = schedules[schedules.len() - 1];
            let sched_start = schedules.iter().fold(sched_start, |sum, e| sum + e.runtime) - sched.runtime;

            Some((sched, last, sched_start))
        })
        .collect_vec(); */

    assert!(results.iter().fold(true, |acc, e| acc && e.len() == results[0].len()));

    let runs = results[0].len();
    let results = results.into_iter()
        .fold(
            vec![vec![]; runs],
            |mut acc, e| {
                e.into_iter().enumerate()
                    .for_each(|(i, e)| acc[i].push(e));
                acc
            }
        );

    let mut ret = true;
    for (results, total_pps) in results.into_iter().zip(total_ppss) {
        // ret = results.iter()
        //     .map(|(sched, last, sched_start)| {
        //         process_result_final(sched, vec![last.clone()], start_unix, *sched_start)
        //     })
        //     .collect_vec()
        //     .into_iter()
        //     .all(|p| p) && ret;

        let (scheds, results, sched_starts) = results.into_iter()
            .fold(
                (vec![], vec![], vec![]),
                |(mut a, mut b, mut c), (ai, bi, ci)| {
                    a.push(ai);
                    b.push(bi);
                    c.push(ci);
                    (a, b, c)
                }
            );

        let sched_start = sched_starts.into_iter().min().unwrap();

        ret = zipf_process_result_final(total_pps, scheds, results, start_unix, sched_start) && ret;
        // eprintln!("\n*******************************************\n");
    }

    ret
    /* schedules
        .iter()
        .zip(packets.into_iter())
        .map(|(schedules, packets)| {
            schedules
                .iter()
                .zip(packets.into_iter())
                .map(|(sched, packet)| {
                    let perthread = if packet.is_some() { vec![packet.unwrap()] } else { vec![] };
                    let r = process_result_final(sched, perthread, start_unix, sched_start);
                    sched_start += sched.runtime;
                    r
                })
                .collect_vec()
                .into_iter()
                .all(|p| p)
        })
        .collect_vec()
        .into_iter()
        .all(|p| p) */
}

fn write_latency_trace_results() {
    let mut cnt_map: HashMap<u64, u64> = HashMap::new();
    let mut sum_lat_map: HashMap<u64, u64> = HashMap::new();
    
    for (ms, lat) in unsafe{ &LATENCY_TRACE_RESULTS } {
        let count = unsafe{ cnt_map.entry(*ms).or_insert(0) };
        *count += 1;

        let sum_lat = unsafe{ sum_lat_map.entry(*ms).or_insert(0) };
        *sum_lat += lat;
    }

    let mut avg_lat_map: HashMap<u64, u64> = HashMap::new();
    for (ms, count) in &cnt_map {
        if let Some(&sum_lat) = sum_lat_map.get(ms) {
            let avg_lat = sum_lat / *count;
            avg_lat_map.insert(*ms, avg_lat);
        }
    }
    // Convert avg_lat_map into a BTreeMap for sorting by keys
    let sorted_avg_lat_map: BTreeMap<u64, u64> = avg_lat_map.into_iter().collect();
    
    if let Some(exptid) = unsafe {&EXPTID} {
        if exptid != "null" {
            let file_path = format!("{}.latency_trace", exptid);
            // Open the file in append mode using OpenOptions
            if let Ok(mut file) = OpenOptions::new()
                .append(true)
                .create(true)
                .open(&file_path) 
            {
                for (ms, avg_lat) in sorted_avg_lat_map.iter() {
                    writeln!(file, "{},{}", ms, avg_lat).expect("Failed to write to file");
                }
            }
        }
        else {
            // Iterate and print the elements in ascending order of keys
            for (ms, avg_lat) in sorted_avg_lat_map.iter() {
                println!("{},{}", ms, avg_lat);
            }
        }
    }
    

    println!("");
}

fn main() {
    let matches = App::new("Synthetic Workload Application")
        .version("0.1")
        .arg(
            Arg::with_name("exptid")
                .long("exptid")
                .value_name("ID")
                .default_value("null")
                .help("Experiment ID"),
        )
        .arg(
            Arg::with_name("ADDR")
                .index(1)
                .multiple(true)
                .help("Address and port to listen on")
                .required(true),
        )
        .arg(
            Arg::with_name("threads")
                .short("t")
                .long("threads")
                .value_name("T")
                .default_value("1")
                .help("Number of client threads"),
        )
        .arg(
            Arg::with_name("discard_pct")
                .long("discard_pct")
                .default_value("10")
                .help("Discard first % of packtets at target QPS from sample"),
        )
        .arg(
            Arg::with_name("mode")
                .short("m")
                .long("mode")
                .value_name("MODE")
                .possible_values(&[
                    "linux-server",
                    "linux-client",
                    "runtime-client",
                    "spawner-server",
                    "local-client",
                ])
                .required(true)
                .requires_ifs(&[("runtime-client", "config"), ("spawner-server", "config")])
                .help("Which mode to run in"),
        )
        .arg(
            Arg::with_name("runtime")
                .short("r")
                .long("runtime")
                .takes_value(true)
                .default_value("10")
                .help("How long the application should run for"),
        )
        .arg(
            Arg::with_name("pps")
                .long("pps")
                .takes_value(true)
                .default_value("10000")
                .help("How many packets should be sent per second"),
        )
        .arg(
            Arg::with_name("start_pps")
                .long("start_pps")
                .takes_value(true)
                .default_value("0")
                .help("Initial rate to sample at"),
        )
        .arg(
            Arg::with_name("config")
                .short("c")
                .long("config")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("protocol")
                .short("p")
                .long("protocol")
                .value_name("PROTOCOL")
                .possible_values(&["synthetic", "memcached", "dns", "reflex", "http", "resp"])
                .default_value("synthetic")
                .help("Server protocol"),
        )
        .arg(
            Arg::with_name("warmup")
                .long("warmup")
                .takes_value(false)
                .help("Run the warmup routine"),
        )
        .arg(
            Arg::with_name("calibrate")
                .long("calibrate")
                .takes_value(true)
                .help("us to calibrate fake work for"),
        )
        .arg(
            Arg::with_name("output")
                .short("o")
                .long("output")
                .value_name("output mode")
                .possible_values(&["silent", "normal", "buckets", "trace"])
                .default_value("normal")
                .help("How to display loadgen results"),
        )
        .arg(
            Arg::with_name("distspec")
                .long("distspec")
                .takes_value(true)
                .help("Distribution of request lengths to use, new format")
                .conflicts_with("distribution")
                .conflicts_with("mean"),
        )
        .arg(
            Arg::with_name("distribution")
                .long("distribution")
                .short("d")
                .takes_value(true)
                .possible_values(&[
                    "zero",
                    "constant",
                    "exponential",
                    "bimodal1",
                    "bimodal2",
                    "bimodal3",
                ])
                .default_value("zero")
                .help("Distribution of request lengths to use"),
        )
        .arg(
            Arg::with_name("mean")
                .long("mean")
                .takes_value(true)
                .default_value("167")
                .help("Mean number of work iterations per request"),
        )
        .arg(
            Arg::with_name("leader-ip")
                .long("leader-ip")
                .takes_value(true)
                .help("IP address of leader instance")
                .conflicts_with("leader"),
        )
        .arg(
            Arg::with_name("barrier-peers")
                .long("barrier-peers")
                .takes_value(true)
                .requires("leader")
                .help("Number of connected loadgen instances"),
        )
        .arg(
            Arg::with_name("leader")
                .long("leader")
                .requires("barrier-peers")
                .takes_value(false)
                .help("Leader of barrier group"),
        )
        .arg(
            Arg::with_name("samples")
                .long("samples")
                .takes_value(true)
                .default_value("20")
                .help("Number of samples to collect"),
        )
        .arg(
            Arg::with_name("fakework")
                .long("fakework")
                .takes_value(true)
                .default_value("stridedmem:1024:7")
                .help("fake worker spec"),
        )
        .arg(
            Arg::with_name("transport")
                .long("transport")
                .takes_value(true)
                .default_value("udp")
                .help("udp or tcp"),
        )
        .arg(
            Arg::with_name("rampup")
                .long("rampup")
                .takes_value(true)
                .default_value("4")
                .help("per-sample ramp up seconds"),
        )
        .arg(
            Arg::with_name("loadshift")
                .long("loadshift")
                .takes_value(true)
                .default_value("")
                .help("loadshift spec"),
        )
        .arg(
            Arg::with_name("live")
                .long("live")
                .takes_value(false)
                .help("run live mode"),
        )
        .arg(
            Arg::with_name("intersample_sleep")
                .long("intersample_sleep")
                .takes_value(true)
                .default_value("0")
                .help("seconds to sleep between samples"),
        )
        .arg(
            Arg::with_name("zipf")
                .long("zipf")
                .takes_value(true)
                .help("enable ZIPF distribution for PPS over threads"),
        )
        .arg(
            Arg::with_name("onoff")
                .long("onoff")
                .takes_value(true)
                .default_value("0")
                .help("enable on/off pattern of requests"),
        )
        .args(&SyntheticProtocol::args())
        .args(&MemcachedProtocol::args())
        .args(&DnsProtocol::args())
        .args(&ReflexProtocol::args())
        .args(&HttpProtocol::args())
        .args(&RespProtocol::args())
        .get_matches();

    let exptid = matches.value_of("exptid").unwrap_or("null").to_string();
    let onoff = matches.value_of("onoff").unwrap_or("0").to_string();
    
    unsafe {
        EXPTID = Some(exptid.clone());
        ONOFF = Some(onoff.clone());
    }

    if let Some(onoff) = unsafe{ &ONOFF } {
        if onoff == "1" {
            eprintln!("Running with On/Off pattern");
        }else {
            eprintln!("Running w/o On/Off pattern");
        }
    }
    let addrs: Vec<SocketAddrV4> = matches
        .values_of("ADDR")
        .unwrap()
        .map(|val| FromStr::from_str(val).unwrap())
        .collect();
    let nthreads = value_t_or_exit!(matches, "threads", usize);
    let discard_pct = value_t_or_exit!(matches, "discard_pct", f32);

    let runtime = Duration::from_secs(value_t!(matches, "runtime", u64).unwrap());
    let packets_per_second = (value_t_or_exit!(matches, "pps", f32)) as usize;
    let start_packets_per_second = (value_t_or_exit!(matches, "start_pps", f32)) as usize;
    assert!(start_packets_per_second <= packets_per_second);
    let config = matches.value_of("config");
    let dowarmup = matches.is_present("warmup");
    let live_mode = matches.is_present("live");

    let distspec = match matches.is_present("distspec") {
        true => value_t_or_exit!(matches, "distspec", String),
        false => {
            let mean = value_t_or_exit!(matches, "mean", f64);
            match matches.value_of("distribution").unwrap() {
                "zero" => "zero".to_string(),
                "constant" => format!("constant:{}", mean),
                "exponential" => format!("exponential:{}", mean),
                "bimodal1" => format!("bimodal:0.9:{}:{}", mean * 0.5, mean * 5.5),
                "bimodal2" => format!("bimodal:0.999:{}:{}", mean * 0.5, mean * 500.5),
                "bimodal3" => format!("bimodal:0.99:{}:{}", mean * 0.5, mean * 5.5),
                _ => unreachable!(),
            }
        }
    };
    let distribution = Distribution::create(&distspec).unwrap();
    let tport = value_t_or_exit!(matches, "transport", Transport);
    let proto: Arc<Box<dyn LoadgenProtocol>> = match matches.value_of("protocol").unwrap() {
        "synthetic" => Arc::new(Box::new(SyntheticProtocol::with_args(&matches, tport))),
        "memcached" => Arc::new(Box::new(MemcachedProtocol::with_args(&matches, tport))),
        "dns" => Arc::new(Box::new(DnsProtocol::with_args(&matches, tport))),
        "reflex" => Arc::new(Box::new(ReflexProtocol::with_args(
            &matches,
            tport,
            distribution,
        ))),
        "http" => Arc::new(Box::new(HttpProtocol::with_args(&matches, tport))),
        "resp" => Arc::new(Box::new(RespProtocol::with_args(&matches, tport))),
        _ => unreachable!(),
    };

    let intersample_sleep = value_t_or_exit!(matches, "intersample_sleep", u64);
    let output = value_t_or_exit!(matches, "output", OutputMode);
    let samples = value_t_or_exit!(matches, "samples", usize);
    let rampup = value_t_or_exit!(matches, "rampup", usize);
    let mode = matches.value_of("mode").unwrap();
    let backend = match mode {
        "linux-server" | "linux-client" => Backend::Linux,
        "spawner-server" | "runtime-client" | "local-client" => Backend::Runtime,
        _ => unreachable!(),
    };

    let loadshift_spec = value_t_or_exit!(matches, "loadshift", String);
    let fwspec = value_t_or_exit!(matches, "fakework", String);
    let fakeworker = Arc::new(FakeWorker::create(&fwspec).unwrap());

    if matches.is_present("calibrate") {
        let us = value_t_or_exit!(matches, "calibrate", u64);
        backend.init_and_run(config, move || {
            let barrier = Arc::new(AtomicUsize::new(nthreads));
            let join_handles: Vec<_> = (0..nthreads)
                .map(|_| {
                    let fakeworker = fakeworker.clone();
                    let barrier = barrier.clone();
                    backend.spawn_thread(move || {
                        barrier.fetch_sub(1, Ordering::SeqCst);
                        while barrier.load(Ordering::SeqCst) > 0 {}
                        fakeworker.calibrate(us);
                    })
                })
                .collect();
            for j in join_handles {
                j.join().unwrap();
            }
        });
        return;
    }

    let zipf = matches.value_of("zipf")
        .map(|alpha| alpha.parse::<f64>().unwrap())
        .filter(|&alpha| if nthreads == 1 {
            panic!("ZIPF distribution was selected with only 1 thread.");
            false
        } else if alpha == 0.0 {
            panic!("Alpha = 0 is a uniform distribution.");
            false
        } else {
            true
        });

    match mode {
        "spawner-server" => match tport {
            Transport::Udp => {
                backend.init_and_run(config, move || run_spawner_server(addrs[0], &fwspec))
            }
            Transport::Tcp => backend.init_and_run(config, move || {
                run_tcp_server(backend, addrs[0], fakeworker)
            }),
        },
        "linux-server" => match tport {
            Transport::Udp => backend.init_and_run(config, move || {
                run_linux_udp_server(backend, addrs[0], nthreads, fakeworker)
            }),
            Transport::Tcp => backend.init_and_run(config, move || {
                run_tcp_server(backend, addrs[0], fakeworker)
            }),
        },
        "local-client" => {
            backend.init_and_run(config, move || {
                println!("Distribution, RPS, Target, Actual, Dropped, Never Sent, Median, 90th, 99th, 99.9th, 99.99th, Start, StartTsc");
                if dowarmup {
                    for packets_per_second in (1..3).map(|i| i * 100000) {
                        let sched = gen_classic_packet_schedule(
                            Duration::from_secs(1),
                            packets_per_second,
                            OutputMode::Silent,
                            distribution,
                            0,
                            nthreads,
                            discard_pct,
                        );
                        run_local(
                            backend,
                            nthreads,
                            fakeworker.clone(),
                            sched,
                        );
                    }
                }
                let step_size = (packets_per_second - start_packets_per_second) / samples;
                for j in 1..=samples {
                    let sched = gen_classic_packet_schedule(
                        runtime,
                        start_packets_per_second + step_size * j,
                        output,
                        distribution,
                        0,
                        nthreads,
                        discard_pct,
                    );
                    run_local(
                        backend,
                        nthreads,
                        fakeworker.clone(),
                        sched,
                    );
                    backend.sleep(Duration::from_secs(3));
                }
            });
        }
        "linux-client" | "runtime-client" => {
            let matches = matches.clone();
            backend.init_and_run(config, move || {

                let mut barrier_group = match (matches.is_present("leader"), matches.value_of("leader-ip")) {
                    (true, _) => {
                        let addr =  SocketAddrV4::new(FromStr::from_str("0.0.0.0").unwrap(), 23232);
                        let npeers = value_t_or_exit!(matches, "barrier-peers", usize);
                        Some(lockstep::Group::new_server(npeers - 1, addr, backend.clone()).unwrap())
                    },
                    (_, Some(ipstr)) => {
                        let addr = SocketAddrV4::new(FromStr::from_str(ipstr).unwrap(), 23232);
                        Some(lockstep::Group::new_client(addr, backend.clone()).unwrap())
                    }
                    (_, _) => None,
                };

                if !live_mode {
                    println!("Distribution, RPS, Target, Actual, Dropped, Never Sent, Median, 90th, 99th, 99.9th, 99.99th, Start, StartTsc");
                }
                match (matches.value_of("protocol").unwrap(), &barrier_group) {
                    (_, Some(lockstep::Group::Client(ref _c))) => (),
                    ("memcached", _) => {
                        let proto = MemcachedProtocol::with_args(&matches, Transport::Tcp);
                        for addr in &addrs {
                            if !run_memcached_preload(proto, backend, Transport::Tcp, *addr, nthreads) {
                                panic!("Could not preload memcached");
                            }
                        }

                    },
                    ("resp", _) => {
                        let protocol = RespProtocol::with_args(&matches, Transport::Tcp);
                        // protocol.preload_servers(backend, Transport::Tcp, addrs[0]);
                    }
                    _ => (),
                };


                if live_mode {
                    let sched = gen_classic_packet_schedule(
                        runtime,
                        packets_per_second,
                        OutputMode::Live,
                        distribution,
                        rampup,
                        nthreads,
                        discard_pct
                    );
                    run_live_client(proto, backend, &addrs, nthreads, tport, &mut barrier_group, sched);
                    unreachable!();
                }

                if !loadshift_spec.is_empty() {
                    if let Some(alpha) = zipf { 
                        let mut rng: StdRng = StdRng::seed_from_u64(292383402);   
                        let (schedules, ppss) = zipf_gen_loadshift_experiment(&loadshift_spec, distribution, nthreads, alpha, output, &mut rng);
                        let schedules = schedules.into_iter().map(|e| Arc::new(e)).collect();
                        eprintln!("\n\nppss: {:?}", ppss);
                        zipf_run_client(
                            ppss,
                            proto,
                            backend,
                            &addrs,
                            nthreads,
                            tport,
                            &mut barrier_group,
                            schedules,
                            1
                        );
                    } else {
                        let sched = gen_loadshift_experiment(&loadshift_spec, distribution, nthreads, output);
                        run_client(
                            proto,
                            backend,
                            &addrs,
                            nthreads,
                            tport,
                            &mut barrier_group,
                            sched,
                            0,
                        );
                    }
                    // write_latency_trace_results();
                    if let Some(ref mut g) = barrier_group {
                        g.barrier();
                    }
                    return;
                }

                if dowarmup {
                    // Run at full pps 3 times for 20 seconds
                    for _ in 0..1 {
                    let sched = gen_classic_packet_schedule(
                        Duration::from_secs(1),
                        (0.75 * (packets_per_second as f64)) as usize,
                        OutputMode::Silent,
                        distribution,
                        20,
                        nthreads,
                        discard_pct,
                    );
                        run_client(
                            proto.clone(),
                            backend,
                            &addrs,
                            nthreads,
                            tport,
                            &mut barrier_group,
                            sched,
                            0,
                        );
                    }
                    backend.sleep(Duration::from_secs(intersample_sleep));
                }

                if let Some(alpha) = zipf {
                    let pps_distribution = get_zipf_distribution(packets_per_second, alpha, nthreads);
                    eprintln!("\n");
                    let schedules = pps_distribution.map(|pps| {
                        Arc::new(zipf_gen_classic_packet_schedule(
                            runtime,
                            pps,
                            output,
                            distribution,
                            rampup,
                            discard_pct
                        ))
                    }).collect_vec();
                    eprintln!("\n");
                    zipf_run_client(
                        vec![packets_per_second],
                        proto,
                        backend,
                        &addrs,
                        nthreads,
                        tport,
                        &mut barrier_group,
                        schedules,
                        1
                    );
                    
                    // write_latency_trace_results();

                    if let Some(ref mut g) = barrier_group {
                        g.barrier();
                    }
                    return;
                }

                let step_size = (packets_per_second - start_packets_per_second) / samples;
                for j in 1..=samples {
                    let sched = gen_classic_packet_schedule(
                        runtime,
                        start_packets_per_second + step_size * j,
                        output,
                        distribution,
                        rampup,
                        nthreads,
                        discard_pct
                    );
                    if !run_client(
                        proto.clone(),
                        backend,
                        &addrs,
                        nthreads,
                        tport,
                        &mut barrier_group,
                        sched,
                        j,
                    ) { break; }
                    if j != samples { backend.sleep(Duration::from_secs(intersample_sleep)); }
                }
                // write_latency_trace_results();
                if let Some(ref mut g) = barrier_group {
                    g.barrier();
                }
            });
        }
        _ => unreachable!(),
    };
}
