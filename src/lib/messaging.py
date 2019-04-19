import socket
import threading

import msg_types

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

def create_get_vote_msg(transaction):
	pass

def parse(msg, addr):
	msg_obj = ast.literal_eval(msg.decode('utf-8'))
	msg_obj['msg_type'] = MSG(msg_obj['msg_type'])
	msg_obj['addr'] = addr
	return msg_obj

def send(msg, ip_addr, port):
	sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
	sock.connect((ip_addr, port))
	nbytes = sock.send(msg)
	sock.close()
	return nbytes

def broadcast(msg, recipients):
	send_threads = []
	for ip_addr, port in recipients:
		t = threading.Thread(target=send, args=(msg, ip_addr, port))
		send_threads.append(t)
	for t in send_threads:
		t.join()
