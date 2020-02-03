#!/bin/bash
export AWS_DEFAULT_REGION=us-west-2

python3 s3-lambda-replay.py \
  -b gamesight-collection-pipeline-us-west-2-prod \
  -p mixer/all/channels/\$LATEST/objects/dt=2019-01-31  \
  -l gstrans-prod-mixer-all-streams \
  -y

# for i in `seq 1 9`;
# do
#   python3 s3-lambda-replay.py \
#   -b gamesight-collection-pipeline-us-west-2-prod \
#   -p mixer/all/channels/\$LATEST/objects/dt=2019-0$i  \
#   -l gstrans-prod-mixer-all-streams,gstrans-prod-mixer-all-games,gstrans-prod-mixer-all-channels,gstrans-prod-mixer-all-game_tag \
#   -y
# done
# for i in `seq 10 12`;
# do
#   python3 s3-lambda-replay.py \
#   -b gamesight-collection-pipeline-us-west-2-prod \
#   -p mixer/all/channels/\$LATEST/objects/dt=2019-$i  \
#   -l gstrans-prod-mixer-all-streams,gstrans-prod-mixer-all-games,gstrans-prod-mixer-all-channels,gstrans-prod-mixer-all-game_tag \
#   -y
# done
# for i in `seq 1 2`;
# do
#   python3 s3-lambda-replay.py \
#   -b gamesight-collection-pipeline-us-west-2-prod \
#   -p mixer/all/channels/\$LATEST/objects/dt=2020-0$i  \
#   -l gstrans-prod-mixer-all-streams,gstrans-prod-mixer-all-games,gstrans-prod-mixer-all-channels,gstrans-prod-mixer-all-game_tag \
#   -y
# done
