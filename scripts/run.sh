#!/bin/sh

# Kill processes if they're still running from previous usage
killall main.exe

# Buid the source
jbuilder build --root ./src main.exe

DIR=./src/_build/default/main.exe
CONFIG=./scripts/config.json

$DIR --node replica --host 127.0.0.1 --port 7010 --config $CONFIG &
$DIR --node replica --host 127.0.0.1 --port 7011 --config $CONFIG &

$DIR --node acceptor --host 127.0.0.1 --port 7030 --config $CONFIG &
$DIR --node acceptor --host 127.0.0.1 --port 7031 --config $CONFIG &
$DIR --node acceptor --host 127.0.0.1 --port 7032 --config $CONFIG &
sleep 1

$DIR --node leader --host 127.0.0.1 --port 7000 --config $CONFIG &
$DIR --node leader --host 127.0.0.1 --port 7001 --config $CONFIG &
sleep 1

$DIR --node client --trace 20 --host 127.0.0.1 --port 7020 --config $CONFIG &
