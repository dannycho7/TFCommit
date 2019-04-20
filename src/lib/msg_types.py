from enum import Enum

class MSG(Enum):
	GET_VOTE = 1
	VOTE = 2

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

def create_msg(msg_type, ip_addr, port, body):
	msg = {
		'msg_type': msg_type.value,
		'body': body
	}
	return str(msg).encode('utf-8')

def create_get_vote_msg(b_i):
	pass

def create_vote_msg(decision, VO_i, root):
	pass


def create_prepare_msg(final_decision, b_i):
	pass