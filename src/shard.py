#! /usr/bin/env python3

import json
import pickle
import socket
import sys
import threading
sys.path.insert(0, './lib')
from blockchain import *
from merkle_tree import MerkleTree, VO_C
from messenger import Messenger
from msg_types import MSG, MessageManager
from cosi import *
from constants import Const

class CurrentExecution:
	def __init__(self, bid):
		self.bid = bid
		self.vote_list = []
		self.ack_resps = []
		self.sch_challenge = None
		self.final_decision = ''

class Shard:
	def __init__(self, global_config, shard_i, mht, bch = Blockchain()):
		self.global_config = global_config
		self.shard_i = shard_i
		self.mht = mht
		self.bch = bch
		self.pending_block = None
		self.current_execution = None
		self.cosi = CoSi()
		self.lock = threading.Lock()
		shard_config = self.global_config['shards'][self.shard_i]
		self.msg_mgr = MessageManager((shard_config['ip_addr'], shard_config['port']))

	def recvEndTransaction(self, req, txn_id, ts, rw_set_list, updates):
		txns = [Transaction(rw_set) for rw_set in rw_set_list]
		self.pending_block = self.bch.createBlock(ts, txns, {})
		self.current_execution = CurrentExecution(ts)
		msg = self.msg_mgr.create_get_vote_msg(self.pending_block, updates)
		Messenger.get().broadcast(msg, self.global_config['shards'])

	def recvGetVote(self, req, block, updates):
		if not self.current_execution or self.current_execution.bid != block.bid:
			self.current_execution = CurrentExecution(block.bid)
		modded_mht = MerkleTree.copyCreate(self.mht)
		for k, new_v in updates:
			modded_mht.update(k, new_v)
		sch_commitment = self.cosi.commitment()
		msg = self.msg_mgr.create_vote_msg(self.shard_i, 'commit', modded_mht.getRoot(), sch_commitment)
		Messenger.get().send(msg, req['addr'])
		# TODO: free modded_mht?

	def recvVote(self, v):
		self.current_execution.vote_list.append(v)
		if len(self.current_execution.vote_list) == len(self.global_config['shards']):
			final_decision = 'commit'
			sch_commits = []
			for vote in self.current_execution.vote_list:
				if vote['decision'] == 'commit':
					self.pending_block.roots[vote['sender_id']] = vote['root']
					sch_commits.append(vote['sch_commitment'])
				else:
					final_decision = 'abort'
			self.current_execution.final_decision = final_decision
			sch_challenge = self.cosi.challenge(str(self.pending_block), sch_commits)
			self.current_execution.sch_challenge = sch_challenge
			msg = self.msg_mgr.create_prepare_msg(final_decision, self.pending_block, sch_challenge)
			Messenger.get().broadcast(msg, self.global_config['shards'])

	def recvPrepare(self, req, body):
		block = pickle.loads(body['block'])
		sch_challenge = body['ch']
		sch_response = self.cosi.response(sch_challenge)
		msg = self.msg_mgr.create_ack_msg(sch_response)
		Messenger.get().send(msg, req['addr'])

	def recvAck(self, res):
		curr_exec = self.current_execution
		curr_exec.ack_resps.append(res)
		if len(curr_exec.ack_resps) == len(self.global_config['shards']):
			aggrR = self.cosi.aggr_response(curr_exec.ack_resps)
			self.pending_block.cosign = tuple((curr_exec.sch_challenge, aggrR))
			msg = self.msg_mgr.create_decision_msg(self.current_execution.final_decision, self.pending_block)
			Messenger.get().send(msg, (self.global_config["client"]["ip_addr"], self.global_config["client"]["port"]))
			Messenger.get().broadcast(msg, self.global_config['shards'])

	def recvDecision(self, final_decision, body):
		if final_decision == 'commit':
			#TODO: Update data
			block = pickle.loads(body['block'])
			self.bch.appendBlock(block)

def handleConnection(sh, client_sock):
	while True:
		req = Messenger.recv(client_sock)
		if req == None:
			break
		print("Recv msg {0}\n".format(req))
		body = req['body']
		sh.lock.acquire()
		if req['msg_type'] == MSG.END_TRANSACTION:
			rw_set_list = pickle.loads(body['rw_set_list'])
			sh.recvEndTransaction(req, body['txn_id'], body['ts'], rw_set_list, body['updates'])
		elif req['msg_type'] == MSG.GET_VOTE:
			block = pickle.loads(body['block'])
			sh.recvGetVote(req, block, body['updates'])
		elif req['msg_type'] == MSG.VOTE:
			sh.recvVote(body)
		elif req['msg_type'] == MSG.PREPARE:
			sh.recvPrepare(req, body)
		elif req['msg_type'] == MSG.ACK:
			sh.recvAck(body['sch_response'])
		elif req['msg_type'] == MSG.DECISION:
			sh.recvDecision(body['final_decision'], body)
		sh.lock.release()

def createMHT(shard_i):
	kv = []
	#TODO: Uncomment next 2 lines when a shard can handle only a subset of data.
	#strt = shard_i * Const.NUM_ELEMENTS + 1
	#for i in range(strt, strt+ Const.NUM_ELEMENTS):
	for i in range(1, Const.NUM_ELEMENTS*Const.NUM_PARTITIONS):
		key = bytes('k'+str(i), 'utf-8')
		val = bytes('v'+str(i), 'utf-8')
		kv.append(key)
		kv.append(val)
	return MerkleTree(kv)

if __name__ == "__main__":
	if len(sys.argv) != 3:
		print("Correct Usage: {0} <config_file_path> <shard_i>".format(sys.argv[0]))
		sys.exit()
	config = json.load(open(sys.argv[1]))
	shard_i = int(sys.argv[2])
	mht = createMHT(shard_i)
	sh = Shard(config, shard_i, mht)

	shard_config = config['shards'][shard_i]

	server_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
	server_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
	server_sock.bind((shard_config['ip_addr'], shard_config['port']))
	server_sock.listen(5)
	while True:
		(client_sock, addr) = server_sock.accept()
		t = threading.Thread(target=handleConnection, args=(sh, client_sock))
		t.start()
