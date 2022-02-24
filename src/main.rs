use std::borrow::Borrow;
use std::fs::File;
use std::io::BufReader;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, mpsc};
use std::thread;
use std::thread::{JoinHandle, Thread};
use std::time::Duration;
use clap::Parser;
use regex::Regex;
use remotefs::RemoteFs;
use remotefs_aws_s3::AwsS3Fs;
use tokio;
use crate::Message::NewJob;


// Arguments struct for command line arguments

#[derive(Parser)]
struct Arguments {
    /// AWS ACCESS KEY
    #[clap(long)]
    aws_access_key_id: String,

    /// AWS SECRET KEY
    #[clap(long)]
    aws_secret_key: String,

    /// REGION
    #[clap(short, long, default_value = "us-east-1")]
    region: String,

    /// Name of the top level s3 bucket to target for searching
    #[clap(short, long)]
    bucket: String,

    /// Number of threads to spawn. Default is 10
    #[clap(short, long)]
    threads: i32,

    /// Switch. If true, s3_search will attempt to decompile JAR files, search for keywords and/or credentials in files.
    #[clap(short, long)]
    deep: bool,

    /// Comma separated list of search terms. e.g. "*password*,credential,*AKIA*,secret"
    #[clap(long)]
    terms: String
}

// ThreadPool struct to manage threads
// Taken from: https://doc.rust-lang.org/book/ch20-02-multithreaded.html

enum Message {
    NewJob(remotefs::fs::File),
    Terminate,
}

pub struct BucketSearch {
    workers: Vec<Worker>,
    sender: Arc<Mutex<mpsc::Sender<Message>>>,
}

impl BucketSearch {
    pub fn new(size: usize, bucket_name: String, region: String, access_key: String, secret: String, keywords: Vec<String>) -> BucketSearch {
        assert!(size > 0);
        let mut workers = Vec::with_capacity(size);
        let (sender, receiver) = mpsc::channel();
        let receiver = Arc::new(Mutex::new(receiver));
        let sender = Arc::new(Mutex::new(sender));
        for id in 0..size {
            workers.push(
                Worker::new(id,
                            Arc::clone(&receiver),
                                     Arc::clone(&sender),
                            bucket_name.to_string(),
                            region.to_string(),
                            access_key.to_string(),
                            secret.to_string(), keywords.to_vec()));
        }

        BucketSearch {workers, sender}
    }

    pub fn execute(&self, dir: remotefs::fs::File) {
        self.sender.lock().unwrap().send(NewJob(dir)).unwrap();
    }
}

impl Drop for BucketSearch {
    fn drop(&mut self) {
        /*if cfg!(debug_assertions) {
            println!("[+] sending terminate message to all workers");
        }

        for _ in &self.workers {
            self.sender.lock().unwrap().send(Message::Terminate).unwrap();
        }

        if cfg!(debug_assertions) {
            println!("[+] shutting down all workers")
        }*/

        for worker in &mut self.workers {
            if cfg!(debug_assertions) {
                //println!("[+] shutting down worker {}", worker.id);
            }

            if let Some(thread) = worker.thread.take() {
                thread.join().unwrap();
            }
        }
    }
}

// Worker for thread pool
struct Worker {
    id: usize,
    thread: Option<thread::JoinHandle<()>>,
}

impl Worker {
    fn new(id: usize, receiver: Arc<Mutex<mpsc::Receiver<Message>>>, sender: Arc<Mutex<mpsc::Sender<Message>>>, bucket_name: String, region: String, access_key: String, secret: String, keywords: Vec<String>) -> Worker {
        let thread = thread::spawn({
            let sender_clone = sender.clone();
            let d = Duration::from_secs(10);
            let mut client = AwsS3Fs::new(bucket_name)
                .region(region)
                .profile("default")
                .access_key(access_key)
                .secret_access_key(secret);

            if client.connect().is_ok() {
                if cfg!(debug_assertions) {
                    println!("[+] successfully connected with s3 client");
                } else {
                    panic!("[!] client failed to connect");
                }
            }
            move ||
                loop {
                    if cfg!(debug_assertions) {
                        println!("[+] worker {} waiting for new job", id);
                    }
                    let message = match receiver.lock().unwrap().recv_timeout(d) {
                        Ok(m) => m,
                        Err(error) => {
                            println!("[!] recv timeout: {}", error);
                            break;
                        }
                    };
                    match message {
                        Message::NewJob(job) => {
                            if cfg!(debug_assertions) {
                                println!("[+] new job received");
                            }
                            let f = job;
                            let results = client.list_dir(f.path()).unwrap();

                            for file in results {
                                if file.borrow().is_file() {
                                    if cfg!(debug_assertions) {
                                        println!("[+] {}", &file.path.to_str().unwrap());
                                    }

                                    for term in &keywords {
                                        let regex_term = regex::escape(&*term);
                                        let regex_str = Regex::new(&*regex_term).unwrap();
                                        //println!("[+] checking regex: {}", regex_str.as_str());
                                        if regex_str.is_match(&*file.name()) {
                                            if cfg!(debug_assertions) {
                                                println!("[+] {} matched {}", file.name(), &term);
                                            }
                                        }
                                    }
                                }

                                if file.borrow().is_dir() {
                                    if cfg!(debug_assertions) {
                                        println!("[+] {}", &file.path.to_str().unwrap());
                                    }
                                    match sender_clone.lock().unwrap().send(NewJob(file)) {
                                        Ok(_) => {
                                            if cfg!(debug_assertions) {
                                                println!("[+] sent job to queue");
                                            }
                                        },
                                        Err(error) => {
                                            panic!("[!] failed to send new job to queue: {}", error);
                                        }
                                    };
                                }
                            }
                        }
                        Message::Terminate => {
                            if cfg!(debug_assertions) {
                                println!("[+] received terminate message");
                            }
                            break;
                        }
                    }
                }
            }
        );

        Worker {
            id,
            thread: Some(thread),
        }
    }
}

// https://stackoverflow.com/questions/30801031/read-a-file-and-get-an-array-of-strings
/*
fn get_search_terms(filename: impl AsRef<Path>) -> Vec<String> {
    let file = File::open(filename).expect("no such file");
    let buf = BufReader::new(file);
    buf.lines()
        .map(|l| l.expect("Could not parse line"))
        .collect()
}*/

#[tokio::main]
async fn main() {
    // Parse all arguments
    let args = Arguments::parse();
    let bucket_name = args.bucket;
    let region_str = args.region;
    let access_key = args.aws_access_key_id;
    let secret = args.aws_secret_key;
    let thread_count = args.threads;
    let keywords : Vec<String> = args.terms.split(",").map(|s| s.to_string()).collect();

    if cfg!(debug_assertions) {
        println!("[+] Targeting bucket: {}", bucket_name);
        println!("[+] Using credentials: {}:{}", access_key, secret);
        println!("[+] Searching for keywords {:?}", keywords);
    }

    let mut client = AwsS3Fs::new(&bucket_name)
        .region(&region_str)
        .profile("default")
        .access_key(&access_key)
        .secret_access_key(&secret);


    if client.connect().is_ok() {
        if cfg!(debug_assertions) {
            println!("[+] successfully authenticated and connected to the target bucket");
        }
    } else {
        if cfg!(debug_assertions) {
            println!("[!] failed to authenticate and connect to S3 bucket");
        }
    }

    let searcher = BucketSearch::new(thread_count.try_into().unwrap(), bucket_name, region_str, access_key, secret, keywords);
    let res = client.list_dir(Path::new("/")).unwrap();
    for item in res {
        if cfg!(debug_assertions) {
            println!("[+] {}", item.path().to_str().unwrap());
        }
        if item.is_dir() {
            searcher.execute(item);
        }
    }
    match client.disconnect() {
        Ok(_) => println!("[+] successfully disconnected from s3"),
        Err(error) => println!("[+] error disconnecting from s3: {}", error),
    };
}
