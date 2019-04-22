from enum import Enum
import pickle

class MSG(Enum):
	END_TRANSACTION = 0
	GET_VOTE = 1
	VOTE = 2
	PREPARE = 3

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
			'msg_type': msg_type.value,
			'addr': self.addr,
			'body': body
		}
		return str(msg).encode('utf-8')

	def create_end_transaction_msg(self, txn_id, ts, rwset, updates):
		end_txn_body = {
			'txn_id': txn_id,
			'ts': ts,
			'rw_set': pickle.dumps(rwset),
			'updates': updates
		}

		return self.create_msg(MSG.END_TRANSACTION, end_txn_body)

	def create_get_vote_msg(self, block, updates):
		get_vote_body = {
			'block': pickle.dumps(block),
			'updates': updates
		}
		
		return self.create_msg(MSG.GET_VOTE, get_vote_body)

	def create_vote_msg(self, decision, root):
		vote_body = {
			'decision': decision,
			'root': root
		}
		
		return self.create_msg(MSG.VOTE, vote_body)


	def create_prepare_msg(self, final_decision, block):
		prepare_body = {
			'final_decision': final_decision,
			'block': pickle.dumps(block)
		}
		
		return self.create_msg(MSG.PREPARE, prepare_body)
