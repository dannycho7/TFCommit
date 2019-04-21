#! /usr/bin/env python3

import json
import sys
sys.path.insert(0, './lib')
import messaging
from msg_types import create_end_transaction_msg

if len(sys.argv) != 2:
	print("Correct Usage: {0} <config_file_path>".format(sys.argv[0]))
	sys.exit()
config = json.load(open(sys.argv[1]))
shard_config = config['shards'][0]
print(shard_config['port'])
messaging.send(create_end_transaction_msg(), shard_config['ip_addr'], shard_config['port'])
