import ast
from msg_types import MSG
import socket
import sys
import threading

MAX_RECV_NBYTES = 2048
MESSAGE_SIZE_HEADER_NBYTES = 4
RETRY_MAX=2

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
	Returns parsed msg if valid msg or None if invalid msg/socket is closed
	"""
	@classmethod
	def recv(cls, sock):
		msg_nbytes_s = sock.recv(MESSAGE_SIZE_HEADER_NBYTES)
		if msg_nbytes_s == b'':
			return None
		msg_nbytes = int.from_bytes(msg_nbytes_s, byteorder=sys.byteorder)
		buf = b''
		while len(buf) != msg_nbytes:
			buf += sock.recv(min(MAX_RECV_NBYTES, msg_nbytes - len(buf)))
		sock.send(msg_nbytes_s) # ACK
		return cls.parse(buf)

	def broadcast(self, msg, recipients):
		send_threads = []
		for recipient in recipients:
			t = threading.Thread(target=self.send, args=(msg, (recipient['ip_addr'], recipient['port'])))
			send_threads.append(t)
			t.start()
		for t in send_threads:
			t.join()

	def _recvAck(self, addr):
		try:
			ack_nbytes = self.socket_map[addr].recv(MESSAGE_SIZE_HEADER_NBYTES)
			return ack_nbytes
		except ConnectionResetError:
			print("ConnectionResetError from {0}".format(addr))
			return 0

	"""
	All messages are pre-pended with a 4-byte integer that indicates the length of the message in bytes.
	"""
	def send(self, msg, addr, ignore_cache=False, retry_left=RETRY_MAX):
		if retry_left <= 0:
			raise ValueError("retry_left must be > 0.")
		self.lock.acquire()
		if ignore_cache or addr not in self.socket_map:
			sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
			sock.connect(addr)
			self.socket_map[addr] = sock
		msg_nbytes = len(msg).to_bytes(MESSAGE_SIZE_HEADER_NBYTES, byteorder=sys.byteorder)
		written_nbytes = 0
		msg_full = msg_nbytes + msg
		while written_nbytes < len(msg_full):
			written_nbytes += self.socket_map[addr].send(msg_full[written_nbytes:])
		ack_nbytes = self._recvAck(addr)
		self.lock.release()
		if ack_nbytes != msg_nbytes:
			if retry_left > 0:
				print("No ack from {0}. Retrying with new socket...".format(addr))
				ack_nbytes = self.send(msg, addr, True, retry_left - 1)
			else:
				print("Out of retries.")
		return ack_nbytes
		#return written_nbytes
