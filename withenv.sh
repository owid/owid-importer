#!/bin/bash -e

if [ $# -lt 2 ]; then
    echo "Usage: $0 <path to env file> <command>"
    exit 1
fi

eval $(egrep '^DB_' "$1" | xargs) "${@:2}"
