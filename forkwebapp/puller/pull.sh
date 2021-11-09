#!/bin/bash

echo "[$(date)] crawling Symbol network"

python -m network.nodes \
	--resources ./networks/symbol.yaml \
	--thread-count 64 \
	--certs ./cert \
	--output ./symbolnodes.json \

echo "[$(date)] downloading Symbol richlist"

python -m network.richlist_symbol \
	--resources ./networks/symbol.yaml \
	--min-balance 250000 \
	--nodes ./symbolnodes.json \
	--output ./250k.csv

echo "[$(date)] crawling NEM network"

python -m network.nodes \
	--resources ./networks/nem.yaml \
	--thread-count 64 \
	--output ./nemnodes.json

echo "[$(date)] downloading NEM harvesters"

python -m network.harvester_nem \
	--resources ./networks/nem.yaml \
	--thread-count 64 \
	--nodes ./nemnodes.json \
	--days 3.5 \
	--output ./nemharvesters.csv
