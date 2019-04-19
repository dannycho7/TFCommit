#! /usr/bin/env python3

import json
from lib.blockchain import *
import lib.messaging as messaging

class Shard:
	def __init__(self, global_conf, shard_i, bch = Blockchain()):
		self.global_conf = global_conf
		self.bch = bch
		self.current_transaction = None
		self.vote_decisions = []
	def broadcast(self, msg):
		messaging.broadcast(msg, self.global_conf['servers'][:shard_i] + self.global_conf['servers'][shard_i+1:])
	def recvEndTransaction(self, mu):
		self.current_transaction = self.bch.createBlock(1, RWSet(), [])
		self.broadcast(create_get_vote_msg(self.current_transaction))
	def recvGetVote(self, vote_req):
		"""
		if message contains valid signature then
			decide to commit or abort T_i
			VO_i <- null
			if decision = commit then
				construct VO_i for the tuples accessed in T_i
			send(signed(vote(decision, VO_i, root_{i-1}))) to vote requester (Coordinator)
		"""
		pass
	def recvVote(self, vote):
		"""
		add to voted store
		check if all involved shards voted
			final_decision <- commit
			corrupted_shards <- {}
			for each shard S_j do
				fetch the last known root last_roots[S_j]
				if root_{i-1} =/= last_roots[S_j] then
					final_decision <- abort
					append S_j to corrupted shards
				else if decision = abort then
					final_decision <- abort
			if final_decision = commit then
				update last_roots[S_j] for each shard S_j
			send signed(decision(T_i, final_decision)) to all shards
		"""
		pass
	def recvPrepare(self, decision, ch, b_i):
		"""
		check if decision is abort
		send Ack to coordinator with response
		"""
		pass
	def recvAck(self, res):
		"""
		aggregate all schnorr-responses to form collective signature
		send b_i and multisig to client and cohorts.
		"""
		pass

if __name__ == "__main__":
	if len(sys.argv) != 2:
		print("Correct Usage: {0} <config_file_path>".format(sys.argv[0]))
		sys.exit()
	config = json.load(open(sys.argv[1]))
	sh = Shard(config)
