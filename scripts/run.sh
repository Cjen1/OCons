#!/bin/sh

DIR=./src/_build/default/main.exe
CONFIG=./scripts/config.json

$DIR --node leader --host 127.0.0.1 --port 7000 --config $CONFIG &
$DIR --node replica --host 127.0.0.1 --port 7010 --config $CONFIG &

$DIR --node acceptor --host 127.0.0.1 --port 7030 --config $CONFIG &
$DIR --node acceptor --host 127.0.0.1 --port 7031 --config $CONFIG &
$DIR --node acceptor --host 127.0.0.1 --port 7032 --config $CONFIG &

sleep 1

$DIR --node client --host 127.0.0.1 --port 7020 --config $CONFIG &
$DIR --node client --host 127.0.0.1 --port 7021 --config $CONFIG &
