#!/bin/sh

mkdir client/logs

echo "----- Building project -----"
cd client/rendezvous
cargo build -r
cp target/release/rendezvous .

cd ../../../emulated_client
cargo build -r

cp target/release/emulated_client ../environment/client
cp target/release/mls-ds ../environment/server

echo "----- Creating Client Docker -----"
cd ../environment/client
docker compose build

echo "----- Creating Server Docker -----"
cd ../server
docker compose build