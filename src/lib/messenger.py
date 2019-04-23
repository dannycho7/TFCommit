import ast
from msg_types import MSG
import socket
import sys
import threading

MIN_RECV_NBYTES = 2048
MESSAGE_SIZE_HEADER_NBYTES = 4

class Messenger:
	__inst = None

	def __init__(self):
		if Messenger.__inst != None:
			raise RuntimeError('Only one instance of Messenger can exist.')
		self.socket_map = {}
		self.lock = threading.Lock()

	@classmethod
	def get(cls):
		if cls.__inst == None:
			cls.__inst = Messenger()
		return cls.__inst

	@staticmethod
	def parse(msg):
		msg_obj = ast.literal_eval(msg.decode('utf-8'))
		msg_obj['msg_type'] = MSG(msg_obj['msg_type'])
		return msg_obj

	"""
	Returns parsed msg if valid msg or None is invalid msg/socket is closed
	"""
	@classmethod
	def recv(cls, sock):
		msg_nbytes_s = sock.recv(MESSAGE_SIZE_HEADER_NBYTES)
		if msg_nbytes_s == b'':
			return None
		msg_nbytes = int.from_bytes(msg_nbytes_s, byteorder=sys.byteorder)
		buf = b''
		while len(buf) != msg_nbytes:
			buf += sock.recv(min(MIN_RECV_NBYTES, msg_nbytes - len(buf)))
		return cls.parse(buf)

	def broadcast(self, msg, recipients):
		send_threads = []
		for recipient in recipients:
			t = threading.Thread(target=self.send, args=(msg, (recipient['ip_addr'], recipient['port'])))
			send_threads.append(t)
			t.start()
		for t in send_threads:
			t.join()

	"""
	All messages are pre-pended with a 4-byte integer that indicates the length of the message in bytes.
	"""
	def send(self, msg, addr):
		self.lock.acquire()
		if addr not in self.socket_map:
			sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
			sock.connect(addr)
			self.socket_map[addr] = sock
		msg_nbytes = len(msg).to_bytes(MESSAGE_SIZE_HEADER_NBYTES, byteorder=sys.byteorder)
		written_nbytes = self.socket_map[addr].send(msg_nbytes + msg)
		self.lock.release()
		return written_nbytes
