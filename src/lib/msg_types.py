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

def create_msg(msg_type, body):
	msg = {
		'msg_type': msg_type.value,
		'body': body
	}
	return str(msg).encode('utf-8')

def create_end_transaction_msg(txn_id, ts, rwset, updates):
	end_txn_body = {
		'txn_id': txn_id,
		'ts': ts,
		'rw_set': pickle.dumps(rwset),
		'updates': updates
	}
	
	return create_msg(MSG.END_TRANSACTION, end_txn_body)

def create_get_vote_msg(b_i):
	return create_msg(MSG.GET_VOTE, '')

def create_vote_msg(decision, VO_i, root):
	pass


def create_prepare_msg(final_decision, b_i):
	pass