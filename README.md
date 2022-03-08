# s3_search

Rust command line tool to rapidly search through s3 buckets for files of interest

### Setup
- Download and install the [AWSCli](https://www.google.com) tool
- Run `aws configure` and provide AWS credentials

### Instructions
```s3_search

USAGE:
    s3_search [OPTIONS] --threads <THREADS> --terms <TERMS> --excludelist <EXCLUDELIST>

OPTIONS:
    -e, --excludelist <EXCLUDELIST>    Comma separated list of file extensions that should be
                                       excluded from file content searches. e.g.
                                       "pdf,docx,txt,tfstate"
    -h, --help                         Print help information
    -m, --maxsize <MAXSIZE>            Max file size in bytes. Default is 1048576 [default: 1048576]
    -p, --profile <PROFILE>            Name of the AWS profile to use in ~/.aws/credentials. Default
                                       value is "default" [default: default]
    -r, --region <REGION>              REGION [default: us-east-1]
    -t, --threads <THREADS>            Number of threads to spawn
        --terms <TERMS>                Comma separated list of search terms. e.g.
                                       "password,credential,AKIA,secret"
    -v, --verbose                      Print verbose output```

