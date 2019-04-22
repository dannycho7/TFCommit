#! /usr/bin/env python3

import json
import sys
sys.path.insert(0, './lib')
from blockchain import RWSet
import messaging
from msg_types import create_end_transaction_msg

if len(sys.argv) != 2:
	print("Correct Usage: {0} <config_file_path>".format(sys.argv[0]))
	sys.exit()
config = json.load(open(sys.argv[1]))
shard_config = config['shards'][0]
print(shard_config['port'])
rw_set = RWSet([], ['k1', hash('v2')])
updates = [('k1', 'v2')]
messaging.send(create_end_transaction_msg(1, 1, rw_set, updates), shard_config['ip_addr'], shard_config['port'])
