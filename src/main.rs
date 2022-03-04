use std::borrow::{Borrow, BorrowMut};
use std::ffi::OsStr;
use std::path::{Path};
use std::sync::{Arc, Mutex, mpsc};
use std::thread;
use std::time::Duration;
use aws_config::default_provider::credentials::DefaultCredentialsChain;
use aws_config::meta::region::RegionProviderChain;
use aws_sdk_s3::model::{Bucket, Object};
use aws_sdk_s3::{Client, Region};
use clap::Parser;
use regex::{Regex, RegexSet};
use regex::internal::Input;
use tokio;
use tokio::runtime::Runtime;
use crate::Message::{NewJobBucket,NewJobObject};


// Arguments struct for command line arguments

#[derive(Parser)]
struct Arguments {
    /// Name of the AWS profile to use in ~/.aws/credentials. Default value is "default"
    #[clap(short, long, default_value = "default")]
    profile: String,

    /// REGION
    #[clap(short, long, default_value = "us-east-1")]
    region: String,

    /// Number of threads to spawn. Default is 10
    #[clap(short, long)]
    threads: i32,

    /// Comma separated list of search terms. e.g. "password,credential,AKIA,secret"
    #[clap(long)]
    terms: String
}

// ThreadPool struct to manage threads
// Taken from: https://doc.rust-lang.org/book/ch20-02-multithreaded.html

// TODO: Add enum for extracting the contents of archive files
enum Message {
    NewJobBucket(Bucket),
    NewJobObject((Object, Bucket)),
    Terminate,
}

// BucketSearch struct to handle thread/worker creation and pool management
pub struct BucketSearch {
    workers: Vec<Worker>,
    sender: Arc<Mutex<mpsc::Sender<Message>>>,
}

impl BucketSearch {
    pub async fn new(size: usize, region_provider: &RegionProviderChain, profile: String, keywords: Vec<String>) -> BucketSearch {
        assert!(size > 0);
        let mut workers = Vec::with_capacity(size);
        let (sender, receiver) = mpsc::channel();
        let receiver = Arc::new(Mutex::new(receiver));
        let sender = Arc::new(Mutex::new(sender));
        let region = region_provider.region().await.unwrap();

        let credentials_provider = DefaultCredentialsChain::builder()
            .region(region)
            .profile_name(&profile)
            .build().await;

        let config = aws_config::from_env()
            .credentials_provider(credentials_provider)
            .load().await;

        let client = aws_sdk_s3::Client::new(&config);
        let client = Arc::new(Mutex::new(client));
        for id in 0..size {
            workers.push(
                Worker::new(id,
                            Arc::clone(&receiver),
                                     Arc::clone(&sender),
                            client.clone(),
                            keywords.to_vec()));
        }

        BucketSearch {workers, sender}
    }

    pub fn execute(&self, buckets: Vec<&Bucket>) {
        for bucket in buckets {
            match self.sender.lock().unwrap().send(NewJobBucket(bucket.clone())) {
                Ok(_) => {},
                Err(_) => {},
            }
        }
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
    fn new(id: usize, receiver: Arc<Mutex<mpsc::Receiver<Message>>>, sender: Arc<Mutex<mpsc::Sender<Message>>>, client: Arc<Mutex<Client>>, keywords: Vec<String>) -> Worker {
        let worker_fn = move || {
            let d = Duration::from_secs(10);
            loop {
                // Wait for a new message from the channel.
                let message = match receiver.lock().unwrap().recv_timeout(d) {
                    Ok(m) => m,
                    Err(error) => {
                        println!("[!] channel timeout for worker {}: {}", id, error);
                        break;
                    }
                };
                match message {
                    // Handle messages from other threads or the main thread
                    Message::NewJobBucket(bucket) => {
                        // TODO: Add call to check bucket ACL
                        // Create the tokio runtime
                        let rt = match Runtime::new() {
                            Ok(rt) => rt,
                            Err(error) => panic!("[!] Unable to create tokio runtime: {}", error),
                        };
                        rt.block_on(Self::handle_bucket(keywords.clone(),client.clone(), bucket.clone(), sender.clone()));
                    }
                    Message::NewJobObject(obj) => {
                        // Search the contents of the file for a match
                        // TODO: Add call to check object ACL
                        // Create the tokio runtime
                        let rt = match Runtime::new() {
                            Ok(rt) => rt,
                            Err(error) => panic!("[!] Unable to create tokio runtime: {}", error),
                        };
                        rt.block_on(Self::handle_object(&keywords, obj.0, obj.1, client.clone()));
                    }
                    Message::Terminate => {
                        if cfg!(debug_assertions) {
                            println!("[+] received terminate message");
                        }
                        break;
                    }
                }
            }
        };
        let thread = thread::spawn( worker_fn);

        Worker {
            id,
            thread: Some(thread),
        }
    }

    async fn handle_bucket(keywords: Vec<String>, client: Arc<Mutex<Client>>, bucket: Bucket, sender: Arc<Mutex<mpsc::Sender<Message>>>) {
        match client.lock() {
            Ok(client) => {
                let resp = match client.list_objects_v2().bucket(bucket.name().unwrap()).send().await {
                    Ok(resp) => resp,
                    Err(error) => panic!("unable to obtain objects for bucket: {}", error),
                };

                for object in resp.contents().unwrap_or_default() {
                    // If an object key doesn't end with / or isn't a directory
                    if !object.key().unwrap().ends_with("/") {
                        if cfg!(debug_assertions) {
                            println!("[+] file: {}", object.key().unwrap());
                        }
                        Self::keyword_match(&keywords, object.key().unwrap().to_string(), bucket.name().unwrap());

                        // Send the content search job to another thread
                        match sender.lock().unwrap().send(NewJobObject((object.clone(), bucket.clone()))) {
                            Ok(_) => {
                                if cfg!(debug_assertions) {
                                    println!("[+] sent job to queue");
                                }
                            },
                            Err(error) => {
                                panic!("[!] failed to send new job to queue: {}", error)
                            },
                        };
                    }
                }
            }
            Err(_) => {},
        }
    }

    async fn handle_object(keywords: &Vec<String>, object: Object, bucket: Bucket, client: Arc<Mutex<Client>>) {

        let set = RegexSet::new(&[
            r"zip",
            r"tar",
            r"7z",
            r"bz2",
            r"tgz",
            r"tbz2",
            r"jar",
        ]).unwrap();

        match client.lock() {
          Ok(client) => {
              if cfg!(debug_assertions) {
                  println!("[+] obtained client lock");
              }
              // TODO: Add logic to limit the file size
              match client.get_object().bucket(bucket.name().unwrap()).key(object.key().unwrap()).send().await {
                  Ok(mut obj_contents) => {
                      let raw = obj_contents.body.collect().await.unwrap().into_bytes().to_vec();
                      if raw.len() > 0 {
                          if cfg!(debug_assertions) {
                              println!("[+] successfully obtained {} bytes for s3://{}/{}",raw.len(),bucket.name().unwrap(),object.key().unwrap());
                          }
                          // Create a path object
                          let path = Path::new(object.key().unwrap());
                          match path.extension() {
                              Some(ext) => {
                                  // If the path has an extension that matches our regex set
                                  if set.is_match(ext.to_str().unwrap()) {
                                      // Extract the contents of the archive
                                      match ext.to_str().unwrap() {
                                          "zip" => {
                                              // Unzip zip archive and search through each file
                                              // TODO: Handle zip archives
                                          },
                                          "jar" => {
                                              // TODO: Handle jar files
                                          },
                                          _ => {}
                                      }
                                  } else {
                                      // TODO: Handle all other extensions
                                      let keyword_set = regex::bytes::RegexSet::new(keywords).unwrap();
                                      if cfg!(debug_assertions) {
                                          println!("[+] searching file s3://{}/{}", bucket.name().unwrap(), object.key().unwrap());
                                      }
                                      if keyword_set.is_match(&*raw) {
                                          println!("[+] Found match in s3://{}/{} for patterns {:?}", bucket.name().unwrap(), object.key().unwrap(), keywords);
                                      } else {
                                          if cfg!(debug_assertions) {
                                              println!("[+] no match found in s3://{}/{} for patterns {:?}", bucket.name().unwrap(), object.key().unwrap(), keywords );
                                          }
                                      }
                                  }
                              }
                              None => {
                                  // TODO: Handle no extension
                                  let keyword_set = regex::bytes::RegexSet::new(keywords).unwrap();
                                  if cfg!(debug_assertions) {
                                      println!("[+] searching file s3://{}/{}", bucket.name().unwrap(), object.key().unwrap());
                                  }
                                  if keyword_set.is_match(&*raw) {
                                      println!("[+] Found match in s3://{}/{} for patterns {:?}", bucket.name().unwrap(), object.key().unwrap(), keywords);
                                  } else {
                                      if cfg!(debug_assertions) {
                                          println!("[+] no match found in s3://{}/{} for patterns {:?}", bucket.name().unwrap(), object.key().unwrap(), keywords );
                                      }
                                  }
                              }
                          };
                      }

                  }
                  Err(error) => {
                      if cfg!(debug_assertions) {
                          println!("[+] unable to get object contents: {}", error);
                      }
                  }
              }
          },
            Err(error) => {
                println!("[!] unable to obtain mutex lock: {}", error);
            }
        };
    }

    fn keyword_match(keywords: &Vec<String>, file_path: String, bucket_name: &str) {
        for term in keywords {
            let regex_term = regex::escape(&*term);
            let regex_str = Regex::new(&*regex_term).unwrap();
            //println!("[+] checking regex: {}", regex_str.as_str());
            if regex_str.is_match(&*file_path) {
                println!("[+] term: {} -> name_match: s3://{}/{}", term, bucket_name, file_path);
            }
        }
    }
}

#[tokio::main]
async fn main() {
    // Parse all arguments
    let args = Arguments::parse();
    let mut region_str: String = args.region;
    let profile = args.profile;
    let thread_count = args.threads;
    let keywords : Vec<String> = args.terms.split(",").map(|s| s.to_string()).collect();
    // Print additional info for debug version
    if cfg!(debug_assertions) {
        println!("[+] Searching for keywords {:?}", keywords);
    }

    let region_provider = RegionProviderChain::first_try(Region::new(region_str))
        .or_default_provider();

    let region = region_provider.region().await.unwrap();

    let credentials_provider = DefaultCredentialsChain::builder()
        .region(region)
        .profile_name(&profile)
        .build().await;

    let config = aws_config::from_env()
        .credentials_provider(credentials_provider)
        .load().await;

    let mut client = aws_sdk_s3::Client::new(&config);

    // List the contents of the root directory in the bucket
    match client.list_buckets().send().await {
        Ok(resp) => {
            // Create a new BucketSearch object. The constructor sets up the desired number of threads and channel
            let searcher = BucketSearch::new(thread_count.try_into().unwrap(),region_provider.borrow(), profile, keywords).await;
            let mut tmp_buckets: Vec<&Bucket> = Vec::new();
            for bucket in resp.buckets().unwrap() {
                tmp_buckets.push(&bucket.borrow());
            }
            searcher.execute(tmp_buckets);
            // Drop the sender part of the channel so that receivers can obtain the next job
            std::mem::drop(searcher.sender.lock().unwrap());
        }
        Err(error) => panic!("[!] Unable to list buckets: {}", error),
    };

}
