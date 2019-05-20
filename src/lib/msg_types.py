from enum import Enum
import pickle

class MSG(Enum):
	REQ_DATA = 0
	RES_DATA = 1
	END_TRANSACTION = 2
	GET_VOTE = 3
	VOTE = 4
	PREPARE = 5
	ACK = 6
	DECISION = 7

"""
All messages will be sent as a utf-8 encoded json object and will have the following format:
{
	'msg_type': int,
	'body': { ... }
}

They will be received, and parsed as: 
{
	'msg_type': MSG,
	'addr': (str, str),
	'body': { ... }
}
"""

class MessageManager:
	def __init__(self, addr):
			self.addr = addr
	def create_msg(self, msg_type, body):
		msg = {
			'addr': self.addr,
			'msg_type': msg_type.value,
			'body': body
		}
		return str(msg).encode('utf-8')

	def create_req_data_msg(self, k):
		req_data_body = {
			'k': k
		}
		return self.create_msg(MSG.REQ_DATA, req_data_body)

	def create_res_data_msg(self, k, data):
		res_data_body = {
			'k': k,
			'data': data
		}
		return self.create_msg(MSG.RES_DATA, res_data_body)

	def create_end_transaction_msg(self, txn):
		end_txn_body = {
			'txn_id': txn.id,
			'ts': txn.ts,
			'rw_set_list': pickle.dumps(txn.rw_set_list),
			'updates': txn.updates
		}
		return self.create_msg(MSG.END_TRANSACTION, end_txn_body)

	def create_get_vote_msg(self, block, updates):
		get_vote_body = {
			'block': pickle.dumps(block),
			'updates': updates
		}
		return self.create_msg(MSG.GET_VOTE, get_vote_body)

	def create_vote_msg(self, sender_id, decision, root, sch_commitment):
		vote_body = {
			'sender_id': sender_id,
			'decision': decision,
			'root': root,
			'sch_commitment': str(sch_commitment)
		}
		return self.create_msg(MSG.VOTE, vote_body)

	def create_prepare_msg(self, final_decision, block, challenge):
		prepare_body = {
			'final_decision': final_decision,
			'block': pickle.dumps(block),
			'ch': challenge
		}
		return self.create_msg(MSG.PREPARE, prepare_body)

	def create_ack_msg(self, sch_response):
		ack_body = {
			'sch_response': sch_response
		}
		return self.create_msg(MSG.ACK, ack_body)

	def create_decision_msg(self, final_decision, block):
		decision_body = {
			'final_decision': final_decision,
			'block': pickle.dumps(block)
		}
		return self.create_msg(MSG.DECISION, decision_body)

class TwoPCMessageManager(MessageManager):

	def create_vote_msg(self, sender_id, decision):
		vote_body = {
			'sender_id': sender_id,
			'decision': decision
		}
		return self.create_msg(MSG.VOTE, vote_body)

	
