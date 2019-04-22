import ast
from msg_types import MSG
import socket
import threading

def parse(msg):
	msg_obj = ast.literal_eval(msg.decode('utf-8'))
	msg_obj['msg_type'] = MSG(msg_obj['msg_type'])
	return msg_obj

def send(msg, addr):
	sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
	sock.connect(addr)
	nbytes = sock.send(msg)
	sock.close()
	return nbytes

def broadcast(msg, recipients):
	send_threads = []
	for recipient in recipients:
		t = threading.Thread(target=send, args=(msg, (recipient['ip_addr'], recipient['port'])))
		send_threads.append(t)
		t.start()
	for t in send_threads:
		t.join()
