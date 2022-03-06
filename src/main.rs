use std::borrow::{Borrow, BorrowMut};
use std::ffi::OsStr;
use std::io::{Read, Write};
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
use grep_matcher::Matcher;
use grep_regex::RegexMatcher;
use grep_searcher::Searcher;
use grep_searcher::sinks::UTF8;
use tempfile::tempfile;
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
    terms: String,

    /// Comma separated list of file types that should be excluded from content searching
    #[clap(long)]
    excludelist: String,

    /// Max file size in bytes. Default is 1048576
    #[clap(short, long, default_value = "1048576")]
    maxsize: i64,

    /// Print verbose output
    #[clap(short, long)]
    verbose: bool,
}

// ThreadPool struct to manage threads
// Taken from: https://doc.rust-lang.org/book/ch20-02-multithreaded.html
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
    pub async fn new(size: usize, region_provider: &RegionProviderChain, profile: String, keywords: Vec<String>, exclude_list: Vec<String>, max_file_size: i64) -> BucketSearch {
        assert!(size > 0);
        // Instantiate the Sender, Receiver, AWS credentials, and AWS client.
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
        // Iterate through the thread count and create a new worker
        for id in 0..size {
            workers.push(
                Worker::new(id,
                            Arc::clone(&receiver),
                                     Arc::clone(&sender),
                            client.clone(),
                            keywords.to_vec(), exclude_list.to_vec(), max_file_size));
        }

        BucketSearch {workers, sender}
    }

    pub fn execute(&self, buckets: Vec<&Bucket>) {
        // Create and send a new job to the channel for each bucket
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
    fn new(id: usize, receiver: Arc<Mutex<mpsc::Receiver<Message>>>, sender: Arc<Mutex<mpsc::Sender<Message>>>, client: Arc<Mutex<Client>>, keywords: Vec<String>, exclude_list: Vec<String>, max_file_size: i64) -> Worker {
        let worker_fn = move || {
            let d = Duration::from_secs(10);
            loop {
                // Wait for a new message/job from the channel.
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
                        rt.block_on(Self::handle_bucket(keywords.clone(),client.clone(), bucket.clone(), sender.clone(), max_file_size));
                    }
                    Message::NewJobObject(obj) => {
                        // Search the contents of the file for a match
                        // TODO: Add call to check object ACL
                        // Create the tokio runtime
                        let rt = match Runtime::new() {
                            Ok(rt) => rt,
                            Err(error) => panic!("[!] Unable to create tokio runtime: {}", error),
                        };
                        rt.block_on(Self::handle_object(&keywords, &exclude_list, obj.0, obj.1, client.clone(), max_file_size));
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

    async fn handle_bucket(keywords: Vec<String>, client: Arc<Mutex<Client>>, bucket: Bucket, sender: Arc<Mutex<mpsc::Sender<Message>>>, max_file_size: i64) {
        // Obtain a lock on the client before use
        match client.lock() {
            Ok(client) => {
                // List all objects in a given bucket
                let resp = match client.list_objects_v2().bucket(bucket.name().unwrap()).send().await {
                    Ok(resp) => resp,
                    Err(error) => panic!("unable to obtain objects for {}: {}", bucket.name().unwrap(), error),
                };
                // Iterate through all objects and confirm its not directory by checking for a trailing slash
                for object in resp.contents().unwrap_or_default() {
                    // If an object key doesn't end with / or isn't a directory
                    if !object.key().unwrap().ends_with("/") {
                        Self::keyword_match(&keywords, object.key().unwrap().to_string(), bucket.name().unwrap());

                        // Send the content search job to another thread
                        match sender.lock().unwrap().send(NewJobObject((object.clone(), bucket.clone()))) {
                            Ok(_) => {},
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

    async fn handle_object(keywords: &Vec<String>, exclude_list: &Vec<String>, object: Object, bucket: Bucket, client: Arc<Mutex<Client>>, max_file_size: i64) {

        let extract_set = RegexSet::new(&[
            r"zip",
            //r"tar",
            //r"7z",
            //r"bz2",
            //r"tgz",
            //r"tbz2",
            r"jar",
        ]).unwrap();

        let exclude_set = RegexSet::new(exclude_list).unwrap();
        // Obtain a lock on the client before use
        match client.lock() {
          Ok(client) => {
              // TODO: Add logic to limit the file size
              match client.head_object().bucket(bucket.name().unwrap()).key(object.key().unwrap()).send().await {
                  Ok(obj_metadata) => {
                      if obj_metadata.content_length <= max_file_size {
                          let path = Path::new(object.key().unwrap());
                          match path.extension() {
                              Some(ext) => {
                                  if exclude_set.is_match(ext.to_str().unwrap()) {
                                     return;
                                  }
                              },
                              None => {},
                          }
                          // Return if the file is more than the max_file_size
                          match client.get_object().bucket(bucket.name().unwrap()).key(object.key().unwrap()).send().await {
                              Ok(mut obj_contents) => {
                                  if obj_contents.content_length < max_file_size {
                                      let raw = obj_contents.body.collect().await.unwrap().into_bytes().to_vec();
                                      if raw.len() > 0 {
                                          match path.extension() {
                                              Some(ext) => {
                                                  // If the path has an extension that matches our regex set
                                                  if extract_set.is_match(ext.to_str().unwrap()) {
                                                      // Extract the contents of the archive
                                                      match ext.to_str().unwrap() {
                                                          "zip" => {
                                                              // Unzip zip archive and search through each file
                                                              // TODO: Handle zip archives
                                                              // Create a tmp file to save the raw contents
                                                              let mut tmp_file = tempfile().unwrap();
                                                              // Write the contents to the tmp file
                                                              match tmp_file.write(&*raw) {
                                                                  Ok(n) => {
                                                                      if cfg!(debug_assertions) {
                                                                          println!("[+] wrote {} bytes for {} to tmpfile", n, object.key().unwrap());
                                                                      }

                                                                      // Open the zip file
                                                                      let mut archive = match zip::ZipArchive::new(tmp_file) {
                                                                          Ok(z) => z,
                                                                          Err(_) => {
                                                                              println!("[!] unable to open zip archive");
                                                                              return;
                                                                          },
                                                                      };
                                                                      // Iterate through all of the files in the ZIP archive
                                                                      for i in 0..archive.len() {
                                                                          let mut file = archive.by_index(i).unwrap();
                                                                          if file.is_file() {
                                                                              Self::keyword_match(&keywords, file.enclosed_name().unwrap().to_str().unwrap().to_string(), format!("{}/{}", bucket.name().unwrap(), object.key().unwrap()).as_str());
                                                                              // Extract the contents
                                                                              let mut tmp_contents= Vec::new();

                                                                              match file.read_to_end(&mut tmp_contents) {
                                                                                  Ok(_) => {}
                                                                                  Err(error) => {
                                                                                      if cfg!(debug_assertions) {
                                                                                          println!("[+] file.read_to_end failed for tmp file {}", error);
                                                                                      }
                                                                                      continue;
                                                                                  }
                                                                              };
                                                                              // Search for byte patterns in the file
                                                                              Self::byte_search(&keywords, format!("{}/{}", object.key().unwrap(), file.enclosed_name().unwrap().to_str().unwrap()), bucket.name().unwrap(), tmp_contents);
                                                                          }
                                                                      }
                                                                  }
                                                                  Err(error) => println!("[!] unable to write content to tmp file: {}", error)
                                                              }


                                                          },
                                                          "jar" => {
                                                              // TODO: Handle jar files
                                                          },
                                                          _ => {}
                                                      }
                                                  } else {
                                                      if cfg!(debug_assertions) {
                                                          println!("[+] searching file s3://{}/{}", bucket.name().unwrap(), object.key().unwrap());
                                                      }
                                                      // Call byte_search for files that have an extension but don't match any pattern in the RegexSet
                                                      Self::byte_search(keywords, object.key().unwrap().to_string(), bucket.name().unwrap(), raw);
                                                  }
                                              }
                                              None => {
                                                  // Match case for files with no extension
                                                  Self::byte_search(keywords, object.key().unwrap().to_string(), bucket.name().unwrap(), raw);
                                              }
                                          };
                                      }
                                  }
                              }
                              Err(error) => {
                                  if cfg!(debug_assertions) {
                                      println!("[+] unable to get object contents: {}", error);
                                  }
                              }
                          }
                      }
                  },
                  Err(error) => {
                      if cfg!(debug_assertions) {
                          println!("[!] head_object function failed: {}", error);
                          return;
                      }
                  }
              }
          },
            Err(error) => {
                println!("[!] unable to obtain mutex lock: {}", error);
            }
        };
    }

    fn byte_search(keywords: &Vec<String>, file_path: String, bucket_name: &str, content: Vec<u8>) {
        // Iterate through all search terms and search for byte patterns that match any term
        for pattern in keywords {
            let matcher = RegexMatcher::new(pattern).unwrap();
            let mut matches: Vec<(u64, String)> = vec![];
            let res = Searcher::new().search_slice(&matcher, &*content, UTF8(|lnum, line| {
                let mymatch = matcher.find(line.as_ref())?.unwrap();
                matches.push((lnum, line[mymatch].to_string()));
                Ok(true)
            }));

            match res {
                Err(error) => println!("[!] byte pattern search failed: {}", error),
                Ok(_) => {
                    if matches.len() > 0 {
                        println!("[+] matches for s3://{}/{}: {:#?}", bucket_name, file_path,matches);
                    }
                }
            }
        }
    }

    fn keyword_match(keywords: &Vec<String>, file_path: String, bucket_name: &str) {
        // Find matches in the object key for any term in the RegexSet
        let set = RegexSet::new(keywords).unwrap();
        if set.is_match(&*file_path) {
            println!("[+] terms: {:#?} -> name_match: s3://{}/{}", set, bucket_name, file_path);
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
    let max_size: i64 = args.maxsize;
    let verbose: bool = args.verbose;
    let keywords : Vec<String> = args.terms.split(",").map(|s| s.to_string()).collect();
    let exclude: Vec<String> = args.excludelist.split(",").map(|s| s.to_string()).collect();
    // Print additional info for debug version
    if cfg!(debug_assertions) {
        println!("[+] Searching for keywords {:?}", keywords);
    }

    // Set the region
    let region_provider = RegionProviderChain::first_try(Region::new(region_str))
        .or_default_provider();

    let region = region_provider.region().await.unwrap();

    // Create the credentials chain with the region and profile
    let credentials_provider = DefaultCredentialsChain::builder()
        .region(region)
        .profile_name(&profile)
        .build().await;

    // Load the configuration
    let config = aws_config::from_env()
        .credentials_provider(credentials_provider)
        .load().await;

    // Create a new client
    let mut client = aws_sdk_s3::Client::new(&config);

    // List all buckets and send the results to the BucketSearch class
    match client.list_buckets().send().await {
        Ok(resp) => {
            // Create a new BucketSearch object. The constructor sets up the desired number of threads and channel
            let searcher = BucketSearch::new(thread_count.try_into().unwrap(),region_provider.borrow(), profile, keywords, exclude,max_size).await;
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
