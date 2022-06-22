#!/bin/bash
set -eu

/bin/pd-server --client-urls http://0.0.0.0:2379 --data-dir "/data/pd" &
/bin/tikv-server --status-addr 0.0.0.0:20180  --pd 0.0.0.0:2379 -A 127.0.0.1:20160 -C "/conf/tikv.toml" -s "/data/tikv"
