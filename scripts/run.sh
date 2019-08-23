#!/bin/sh

# Kill processes if they're still running from previous usage
killall main.exe

# Buid the source
dune build --root ./src main.exe

DIR=./src/_build/default/main.exe
CONFIG=./scripts/config.json

$DIR --node replica --host 127.0.0.1 --port 50010 --config $CONFIG &
$DIR --node replica --host 127.0.0.1 --port 50011 --config $CONFIG &

$DIR --node acceptor --host 127.0.0.1 --port 50030 --config $CONFIG &
$DIR --node acceptor --host 127.0.0.1 --port 50031 --config $CONFIG &
$DIR --node acceptor --host 127.0.0.1 --port 50032 --config $CONFIG &
sleep 1

$DIR --node leader --host 127.0.0.1 --port 50000 --config $CONFIG &
$DIR --node leader --host 127.0.0.1 --port 50001 --config $CONFIG &
sleep 1

$DIR --node client --trace 20 --host 127.0.0.1 --port 50020 --config $CONFIG &
