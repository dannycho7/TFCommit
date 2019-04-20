#! /usr/bin/env python3

import ast
import json
import socket
from lib.blockchain import *
import lib.messaging as messaging
import lib.msg_types as msg_types

class Shard:
	def __init__(self, global_conf, shard_i, mht, bch = Blockchain()):
		self.global_conf = global_conf
		self.bch = bch
		self.mht = mht
		self.current_transaction = None
		self.vote_decisions = []
	def broadcast(self, msg):
		messaging.broadcast(msg, self.global_conf['servers'][:shard_i] + self.global_conf['servers'][shard_i+1:])
	def recvEndTransaction(self, req, txn_id, ts, rwset):
		self.current_transaction = self.bch.createBlock(txn_id, rwset, [])
		self.broadcast(create_get_vote_msg(self.current_transaction))
	def recvGetVote(self, req, b_i):
		modded_mht = MerkleTree.copyCreate(self.mht)
		# TODO: modify modded_mht based on rw-set in b_i
		ip_addr, port = req['addr']
		messaging.send(ip_addr, port, create_vote_msg(decision, VO_i, self.mht.getRoot()))
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
	if len(sys.argv) != 3:
		print("Correct Usage: {0} <config_file_path> <shard_i>".format(sys.argv[0]))
		sys.exit()
	config = json.load(open(sys.argv[1]))
	shard_i = sys.argv[2]
	sh = Shard(config, shard_i)

	server_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
	server_sock.bind((config['shards'][shard_i], config['shards'][shard_i]))
	server_sock.listen(5)
	while True:
		(client_sock, addr) = server_sock.accept()
		req = messaging.parse(client_sock.recv(2048), addr)
		if req['msg_type'] == MSG.GET_VOTE:
			pass
