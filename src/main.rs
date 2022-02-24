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

    /// Comma separated list of search terms. e.g. "password,credential,AKIA,secret"
    #[clap(long)]
    terms: String
}

// ThreadPool struct to manage threads
// Taken from: https://doc.rust-lang.org/book/ch20-02-multithreaded.html

// TODO: Add enum for extracting the contents of archive files
enum Message {
    NewJob(remotefs::fs::File),
    Terminate,
}

// BucketSearch struct to handle thread/worker creation and pool management
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
        for worker in &mut self.workers {

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
            let d = Duration::from_secs(10); // Timeout value for idle threads
            // Create a new s3 client
            let mut client = AwsS3Fs::new(bucket_name)
                .region(region)
                .profile("default")
                .access_key(access_key)
                .secret_access_key(secret);

            // Validate the client connection. Panic if unable to connect
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
                    // Wait for a new message from the channel.
                    let message = match receiver.lock().unwrap().recv_timeout(d) {
                        Ok(m) => m,
                        Err(error) => {
                            println!("[!] recv timeout: {}", error);
                            break;
                        }
                    };
                    match message {
                        // If the message is for a new directory, add it to the job queue
                        // TODO: Handle jobs that contain files or directories
                        Message::NewJob(job) => {
                            if cfg!(debug_assertions) {
                                println!("[+] worker {} received new job", id);
                            }
                            let f = job;
                            let results = client.list_dir(f.path()).unwrap();
                            // Iterate through the directory listing
                            for file in results {
                                if file.borrow().is_file() {
                                    if cfg!(debug_assertions) {
                                        println!("[+] {}", &file.path.to_str().unwrap());
                                    }
                                    // If an entry is a file, iterate through the keyword list and check for a match
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
                                // If an entry is a directory, add it to the job queue
                                if file.borrow().is_dir() {
                                    if cfg!(debug_assertions) {
                                        println!("[+] {}", &file.path.to_str().unwrap());
                                    }
                                    let sender_clone = sender.clone();
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
                                    std::mem::drop(sender_clone.lock().unwrap());
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
    // Print additional info for debug version
    if cfg!(debug_assertions) {
        println!("[+] Targeting bucket: {}", bucket_name);
        println!("[+] Using credentials: {}:{}", access_key, secret);
        println!("[+] Searching for keywords {:?}", keywords);
    }
    // Create a templorary S3 client to validate the credentials and bucket
    let mut client = AwsS3Fs::new(&bucket_name)
        .region(&region_str)
        .profile("default")
        .access_key(&access_key)
        .secret_access_key(&secret);

    // Test the client connection
    if client.connect().is_ok() {
        if cfg!(debug_assertions) {
            println!("[+] successfully authenticated and connected to the target bucket");
        }
    } else {
        if cfg!(debug_assertions) {
            println!("[!] failed to authenticate and connect to S3 bucket");
        }
    }
    // Create a new BucketSearch object. The constructor sets up the desired number of threads and channel
    let searcher = BucketSearch::new(thread_count.try_into().unwrap(), bucket_name, region_str, access_key, secret, keywords);
    // List the contents of the root directory in the bucket
    let res = client.list_dir(Path::new("/")).unwrap();
    // Iterate through the bucket contents
    // TODO: Pass all file objects to the searcher.execute function
    for item in res {
        if cfg!(debug_assertions) {
            println!("[+] {}", item.path().to_str().unwrap());
        }
        if item.is_dir() {
            searcher.execute(item);
        }
    }
    // Drop the sender part of the channel so that receivers can obtain the next job
    std::mem::drop(searcher.sender.lock().unwrap());
    match client.disconnect() {
        Ok(_) => println!("[+] successfully disconnected from s3"),
        Err(error) => println!("[+] error disconnecting from s3: {}", error),
    };
}
