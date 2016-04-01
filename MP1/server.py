#server
import socket
import threading
import time
import Queue
import sys

#Divided Classes and function for refactoring

# Basic variables
# 1. socket
# 2. queue fore each destination including ack
# 3. ack_count list to check whether enough number of ack has been arrived
# 4. ack_ok list to check how many ack is required(Linear and Sequential needs 4, eventual needs 1 or 2 depend on model)
# 5. version is needed to count correct number of ack
# 6. waiting to ensure totally-ordered broadcast
# 7. time_list, value_list, and toggle is for getting command with model 4.  It stores the value and the timestamp so that it can pick the recent one
sock = 5417
q = Queue.Queue(maxsize=0)
ack_count = [0, 0, 0, 0]
ack_ok = [4, 4, 4, 4]
version = [0, 0, 0, 0]
waiting = [False] #this is only for linearizability and sequential
time_list = [0, 0]
value_list = [' ', ' ']
toggle = [0]

#receiving class that runs the thread to receive any data
class receiving(threading.Thread):
    def __init__(self, receive_soc, client, client_num, dest, time_list, value_list):
        threading.Thread.__init__(self)
        self.receive_soc = receive_soc
	self.dest = dest
	self.client = client
	self.client_num = client_num
        self.stopIt = False
	self.toggle = 0
	self.time_list = time_list
	self.value_list = value_list

    def mrecv(self):
	msg = self.receive_soc.recv(50)
	return msg

    def run(self):
        while not self.stopIt:
	    message = self.mrecv()
	    msg = message.split()  

	    print 'msg', msg

	    #If it sees the send, simply send it because it doesn't need to wait for the ack
	    if msg[0] == 'Send':
		real_msg = ' '.join(msg[1:-2])
	    	rmsg = 'MSG ' + str(real_msg) + ' ' + self.client + ' ' + str(msg[-1])
	    	if msg[-2] == 'A':
			msend(self.dest[0], rmsg)
	    	elif msg[-2] == 'B':
			msend(self.dest[1], rmsg)
	   	elif msg[-2] == 'C':
			msend(self.dest[2], rmsg)
	    	elif msg[-2] == 'D':
			msend(self.dest[3], rmsg)

	    #search immediately request for the value and sends back the result
	    elif msg[0] == 'search':
		for x in range(0, 4):
	    	    msend(dest[x], message)
		A_value = dest[0].recv(50)
		B_value = dest[1].recv(50)
		C_value = dest[2].recv(50)
		D_value = dest[3].recv(50)

		rtnString = 'A = ' + A_value + ' B = ' + B_value + ' C = ' + C_value + ' D = ' + D_value 
		self.receive_soc.send(rtnString)
		    
	    #If the message is the ack, increment the ack_count for each server
	    elif msg[0] == 'ack':
		dest_num = int(msg[1])
		if int(msg[-1]) == version[dest_num]:
			ack_count[dest_num] = ack_count[dest_num] + 1
			if len(msg) == 6 and ack_ok[dest_num] == 2: #get for eventual. For R=2, needs to compare the timestamp		
				self.time_list[toggle[0]] = float(msg[4])
				self.value_list[toggle[0]] = msg[3]
				toggle[0] = 1 - toggle[0]

				print self.time_list				
				print self.value_list

		#If the number of ack is enough for each server to process next operation, send ack to that server
		if ack_count[dest_num] >= ack_ok[dest_num]:
			if len(msg) == 4: #get for linear and sequential
				msend(dest[dest_num], 'ack' + ' ' + str(msg[2]))#ack <key>
			elif len(msg) == 6 and ack_ok[dest_num] == 1: #get for eventual
				msend(dest[dest_num], 'ack' + ' ' + str(msg[2]) + ' ' + str(msg[3]))#ack <key> <value>
			elif len(msg) == 6 and ack_ok[dest_num] == 2: #get for eventual
				value = None				
				if self.time_list[0] > self.time_list[1]: #get the recent value
					value = self.value_list[0]
				else:
					value = self.value_list[1]
				
				msend(dest[dest_num], 'ack' + ' ' + str(msg[2]) + ' ' + str(value))#ack <key> <value>
				self.time_list = [0, 0]
				self.value_list = [' ', ' ']
			else:
				msend(dest[dest_num], 'ack')
			ack_count[dest_num] = 0
			version[dest_num] = version[dest_num] + 1
			waiting[0] = False
	    	
	    else:
		message = message + ' ' + str(self.client_num)
				
		q.put(message)

#queue thread that waits for any data coming to coordinate server
class qu_waiter(threading.Thread):
    def __init__(self, dest):
        threading.Thread.__init__(self)
        self.stopIt = False
	self.dest = dest

    def run(self):
        while 1:
            if(q.empty() == False):
		q_worker(q, self.dest)

def msend(conn,msg):
        conn.send(msg)

#for linearizability and sequatial consistency, we need to wait for all the ack to be processed
def linear_sequential(msg, dest):
	waiting[0] = True
	for x in range(0, 4):
	    msend(dest[x], msg)

#for eventual, send the message as soon as the coordinate servers gets it and wait for enough number of ack
def eventual(message, dest):
	msg = message.split()
	if msg[-2] == 'A':
	        msend(dest[0], message)
	elif msg[-2] == 'B':
	        msend(dest[1], message)
	elif msg[-2] == 'C':
	        msend(dest[2], message)
	elif msg[-2] == 'D':
	        msend(dest[3], message)

#function that check what model is in order to update information
def q_worker(qu, dest):
    if qu.empty() == True:
        return
    else:
        message = qu.get(False)

	while 1:
	    if waiting[0] == False:
		break

	msg = message.split()

	#linear and sequential
	if msg[0] == 'delete':
		linear_sequential(message, dest)
	elif msg[0] == 'get' and msg[2] == '1':
		ack_ok[int(msg[4])] = 4
		linear_sequential(message, dest)
	elif msg[0] == 'insert' and msg[3] == '1':
		ack_ok[int(msg[5])] = 4
		linear_sequential(message, dest)
	elif msg[0] == 'update' and msg[3] == '1':
		ack_ok[int(msg[5])] = 4
		linear_sequential(message, dest)
	elif msg[0] == 'insert' and msg[3] == '2':
		ack_ok[int(msg[5])] = 4
		linear_sequential(message, dest)
	elif msg[0] == 'update' and msg[3] == '2':
		ack_ok[int(msg[5])] = 4
		linear_sequential(message, dest)

	#eventual consistency
	elif msg[0] == 'get' and msg[2] == '3':
		ack_ok[int(msg[5])] = 1
		eventual(message, dest)
	elif msg[0] == 'get' and msg[2] == '4':
		ack_ok[int(msg[5])] = 2
		eventual(message, dest)
	elif msg[0] == 'insert' and msg[3] == '3':
		ack_ok[int(msg[6])] = 1
		eventual(message, dest)
	elif msg[0] == 'insert' and msg[3] == '4':
		ack_ok[int(msg[6])] = 2
		eventual(message, dest)
	elif msg[0] == 'update' and msg[3] == '3':
		ack_ok[int(msg[6])] = 1
		eventual(message, dest)
	elif msg[0] == 'update' and msg[3] == '4':
		ack_ok[int(msg[6])] = 2
		eventual(message, dest)

#manin socket that accepts the connection
soc = socket.socket(socket.AF_INET,socket.SOCK_STREAM)
soc.bind(('127.0.0.1',sock))
soc.listen(5)

#The name of each server is decided in the order that it is connected
(c1_receive,a1) = soc.accept()
(c1_send,a2) = soc.accept()
c1_send.send('A')
(c2_receive,a3) = soc.accept()
(c2_send,a4) = soc.accept()
c2_send.send('B')
(c3_receive,a5) = soc.accept()
(c3_send,a6) = soc.accept()
c3_send.send('C')
(c4_receive,a7) = soc.accept()
(c4_send,a8) = soc.accept()
c4_send.send('D')

#store information about the sockets
dest = [c1_send, c2_send, c3_send, c4_send]

thr1 = receiving(c1_receive, 'A', 0, dest, time_list, value_list)
thr1.start()
thr2 = receiving(c2_receive, 'B', 1, dest, time_list, value_list)
thr2.start()
thr3 = receiving(c3_receive, 'C', 2, dest, time_list, value_list)
thr3.start()
thr4 = receiving(c4_receive, 'D', 3, dest, time_list, value_list)
thr4.start()

thr5 = qu_waiter(dest)
thr5.start()

try:
    while 1:
	msg = raw_input()
        if msg == 'exit':
	    break
except:
    print 'closing'

thr1.stopIt=True
thr2.stopIt=True
thr3.stopIt=True
thr4.stopIt=True

thr1.conn.close()
thr2.conn.close()
thr3.conn.close()
thr4.conn.close()

soc.close()
sys.exit()
