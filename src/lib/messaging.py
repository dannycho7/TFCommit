import ast
from msg_types import MSG
import socket
import threading

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
