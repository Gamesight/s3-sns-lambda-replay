# s3-sns-lambda-replay
A simple tool for replaying S3 file creation Lambda invocations. This is useful for backfill or replay on real-time ETL pipelines that run transformations in Lambdas triggered by S3 file creation events.

Steps:
  1. Collect inputs from user
  2. Scan S3 for filenames that need to be replayed
  3. Batch S3 files into payloads for Lambda invocations
  4. Spawn workers to handle individual Lambda invocations/retries
  5. Process the work queue, keeping track of progress in a file in case of interrupts

## Getting Started
1. First step is to setup a python3 venv to hold our deps
```
./setup-venv.sh
```
2. Run the command and follow the prompts
```
python s3-lambda-replay.py
```

## Options
Run the help command for a full list of available command line options
```
python s3-lambda-replay.py --help
```
## Example Command Line Execution
Note that we must escape the `$`
This example is also included in the file `run_replay.sh`
```
python3 s3-lambda-replay.py \
-b gamesight-collection-pipeline-us-west-2-prod \
-p twitch/all/chatters/\$LATEST/objects/dt=2020-01-02-08-00/,twitch/all/chatters/\$LATEST/objects/dt=2020-01-02-09-00/ \
-l gstrans-prod-twitch-all-chatters
```

Using the command line also allows us to quickly include paths that aren't along `/` separation lines. For example, we can use the path `twitch/all/chatters/\$LATEST/objects/dt=2020-01-02-1` to look at all records between 10:00 and 20:00 on 2020-01-02, or `twitch/all/chatters/\$LATEST/objects/dt=2020-01-02` to just run all of the objects from that day.
