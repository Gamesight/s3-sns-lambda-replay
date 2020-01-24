!/bin/bash
for i in `seq 8 9`;
do
  python3 s3-lambda-replay.py \
  -b gamesight-collection-pipeline-us-west-2-prod \
  -p twitch/all/chatters/\$LATEST/objects/dt=2020-01-0$i \
  -l gstrans-prod-twitch-all-chatters \
  -y
done


for i in `seq 10 24`;
do
  python3 s3-lambda-replay.py \
  -b gamesight-collection-pipeline-us-west-2-prod \
  -p twitch/all/chatters/\$LATEST/objects/dt=2020-01-$i \
  -l gstrans-prod-twitch-all-chatters \
  -y
done
