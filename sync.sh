#!/bin/bash -e
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
RSYNC="rsync -havz --no-perms --progress --delete --exclude-from=$DIR/.rsync-ignore"
HOST="owid@terra"
ROOT="/home/owid"
NAME="importer"

SYNC_TARGET="$ROOT/$NAME"

# Rsync the local repository to a temporary location on the server
$RSYNC $DIR/ $HOST:$SYNC_TARGET

# Install dependencies
ssh -t $HOST 'bash -e -s' <<EOF
cd $SYNC_TARGET
source env/bin/activate && pip install -r requirements.txt
EOF

