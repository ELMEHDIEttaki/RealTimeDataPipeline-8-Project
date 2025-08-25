#!/bin/bash

HOST=$1
PORT=$2
TIMEOUT=${3:-180}  # default timeout of 180 seconds
START_TIME=$(date +%s)

echo "⏳ Waiting for Kafka at $HOST:$PORT..."

while ! nc -z $HOST $PORT; do
  sleep 1
  NOW=$(date +%s)
  ELAPSED=$((NOW - START_TIME))
  if [ $ELAPSED -ge $TIMEOUT ]; then
    echo "Timeout after ${TIMEOUT}s waiting for $HOST:$PORT"
    exit 1
  fi
done

echo "Kafka is up at $HOST:$PORT"

exec "${@:4}"  # run the rest of the command (e.g., python script)
