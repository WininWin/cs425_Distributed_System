#client
import socket
import threading
import time
import Queue
import random
import sys

#Divided Classes and function for refactoring

# Basic variables
# 1. socket
# 2. MAX delay
# 3. queue fore each destination including ack
# 4. list that holds information about delays
# 5. dictionary to store key-value
# 6. dictionary to store key-timestamp
# 7. waiting list that insures FIFO for each destination
# 8. version is for eventual consistency.
sock = 5417
MAX = 10.0
qA = Queue.Queue(maxsize=0)
qB = Queue.Queue(maxsize=0)
qC = Queue.Queue(maxsize=0)
qD = Queue.Queue(maxsize=0)
q_ack = Queue.Queue(maxsize=0)
actual_time_list_A = [0]
actual_time_list_B = [0]
actual_time_list_C = [0]
actual_time_list_D = [0]
actual_time_list_ack = [0]
main_dict = { }
even_dict = { }
waiting = [False, False, False, False, False]
version = [0]

#Connect with coordinate server. one sock for receiving and one for sending
#Note: Even though I use two socket, my program runs as if there is only one socket.  I used two socket just for my understanding
send_soc = socket.socket(socket.AF_INET,socket.SOCK_STREAM)
send_soc.connect(('127.0.0.1',sock))

receive_soc = socket.socket(socket.AF_INET,socket.SOCK_STREAM)
receive_soc.connect(('127.0.0.1',sock))

#Communication between coordinate server and current server to assign the name of the server
#The name is decided in the order that the server is connected to the coordinate server
server_name = receive_soc.recv(1)
print '----------This is Server', server_name, '----------'

#Receiving thread that receives the msg from coord server
class receiving(threading.Thread):
    def __init__(self,receive_soc):
        threading.Thread.__init__(self)
        self.receive_soc = receive_soc
        self.stopIt = False

    def mrecv(self):
	msg = self.receive_soc.recv(50)
	return msg
    def run(self):
        while not self.stopIt:
            message = self.mrecv()
	    msg = message.split()

	    if msg[0] == 'MSG':
		real_msg = ' '.join(msg[1:-2])
            	print 'Received \"' + str(real_msg) + '\" from ' + str(msg[-2]) + ', Max delay is ' + str(msg[-1]) + ' s, system time is ' + str(time.time())

	    #when it receives the ack
	    elif msg[0] == 'ack':
		for x in range(0, 4):
		    waiting[x] = False

		if len(msg) == 2:
		    value = main_dict.get(msg[1])
		    print 'get(' + str(msg[1]) + ') = ' + str(value)

		elif len(msg) == 3:
		    print 'get(' + str(msg[1]) + ') = ' + str(msg[2])
		
		print 'received ack'
	
	    elif msg[0] == 'search':
		value = main_dict.get(msg[1])
		if value == None:
		    value = 'No Key Found'	
		msend(self.receive_soc, value)

	    else:
		command_worker(msg)

#Queue thread that waits for the command to be putted in the queue and handles delay
class qu_waiter(threading.Thread):
    def __init__(self, send_soc, qu, actual_time_list, thread_num):
        threading.Thread.__init__(self)
        self.stopIt = False
	self.send_soc = send_soc
	self.qu = qu
	self.actual_time_list = actual_time_list
	self.thread_num = thread_num

    def run(self):
        while 1:
            if(self.qu.empty() == False):
                q_sheduler(send_soc, self.qu, self.actual_time_list, self.thread_num)

def msend(conn,msg):
        conn.send(msg)

#Function that handles the msg in the queue
def q_sheduler(send_soc, qu, actual_time_list, thread_num):
    if qu.empty() == True:
        return
    else:
        msg = qu.get(False)

	while 1:
	    if waiting[thread_num] == False:
		break

        #command for delay
        msg_split = msg.split()
        if msg_split[0] == 'delay' and int(msg_split[1])>0:
            time.sleep(int(msg_split[1]))
            actual_time_list.pop()	
	    return None
	#if it is simple Sending message, add MAX delay at the end
	elif msg_split[0] == 'Send':
	    msg = msg + ' ' + str(MAX)        

	#calculate the max_time(Equation given in the description)
        max_time = max(actual_time_list[len(actual_time_list)-2], actual_time_list[len(actual_time_list)-1])

	actual_time_list[len(actual_time_list)-2] = max_time
        current_time = time.time()
        actual_delay = max_time - current_time
	if actual_delay < 0:
	    actual_delay = 0.00001
	
	time.sleep(actual_delay) #sleep for the delay(This does not prevent receiving and typing the command because it is seperated thread)
        actual_time_list.pop()
       
	#stops thread to ensure FIFO
	if msg_split[0] != 'Send' and msg_split[0] != 'ack':
	    waiting[thread_num] = True
	
	msend(send_soc, msg)

#function that handles the msg that is received
# Basic format for ack message
# ack <dest> <version>
def command_worker(msg):
    if msg[0] == 'delete':
	if msg[1] in main_dict:
            del main_dict[msg[1]]	
	    put_in_qu('ack' + ' ' + str(msg[3]) + ' ' + str(msg[2]))
	    print 'Key ' + str(msg[1]) + ' deleted'
	else:
	    put_in_qu('ack' + ' ' + str(msg[3]) + ' ' + str(msg[2]))
	    print 'no key found'

    #ack for get is special because it needs more information
    elif msg[0] == 'get':
        if msg[2] == '1':
	    put_in_qu('ack' + ' ' + str(msg[4]) + ' ' + str(msg[1]) + ' ' + str(msg[3]))#ack <dest> <key> <version>
	
	#ack <dest> <key> <value> <time> <version>
	elif msg[2] == '3' or msg[2] == '4':
	    put_in_qu('ack' + ' ' + str(msg[5]) + ' ' + str(msg[1]) + ' ' + str(main_dict.get(msg[1])) + ' ' + str(even_dict[msg[1]]) + ' ' + str(msg[3])) 

    elif msg[0] == 'insert':
	main_dict[msg[1]] = msg[2]

	if msg[3] == '1' or msg[3] == '2':
	    put_in_qu('ack' + ' ' + str(msg[5]) + ' ' + str(msg[4]))

	elif msg[3] == '3' or msg[3] == '4':
	    even_dict[msg[1]] = time.time() #save timestamp (Just for eventual)
	    put_in_qu('ack' + ' ' + str(msg[6]) + ' ' + str(msg[4]))
	
        print 'Inserted key ' + str(msg[1])

    elif msg[0] == 'update':
        if msg[1] in main_dict:
	    old = main_dict.get(msg[1])
	    main_dict[msg[1]] = msg[2]
	    
	    if msg[3] == '1' or msg[3] == '2':
	    	put_in_qu('ack' + ' ' + str(msg[5]) + ' ' + str(msg[4]))

	    elif msg[3] == '3' or msg[3] == '4':
	        even_dict[msg[1]] = time.time()  #save timestamp (Just for eventual)
            	put_in_qu('ack' + ' ' + str(msg[6]) + ' ' + str(msg[4]))
	    
            print 'Key ' + str(msg[1]) + ' changed from ' + str(old) + ' to ' + str(msg[2])
        else:
	    if msg[3] == '1' or msg[3] == '2':
	    	put_in_qu('ack' + ' ' + str(msg[5]) + ' ' + str(msg[4]))

	    elif msg[3] == '3' or msg[3] == '4':
            	put_in_qu('ack' + ' ' + str(msg[6]) + ' ' + str(msg[4]))

            print 'No key found'
    
    else:
        return 'Invalid input or delay'

#function that put message in the queue
def put_in_qu(message):
	msg = message.split()
	delay_time = random.uniform(0.0, MAX)
        actual_time = time.time() + delay_time

	if msg[-1] == 'A':
		actual_time_list_A.insert(0, actual_time)
        	qA.put(message)
	elif msg[-1] == 'B':
		actual_time_list_B.insert(0, actual_time)
        	qB.put(message)
	elif msg[-1] == 'C':
		actual_time_list_C.insert(0, actual_time)
        	qC.put(message)
	elif msg[-1] == 'D':
		actual_time_list_D.insert(0, actual_time)
        	qD.put(message)
	elif msg[0] == 'ack':
		actual_time_list_ack.insert(0, actual_time)
        	q_ack.put(message)
	else:
		actual_time_list_A.insert(0, actual_time)
		qA.put(message)

#Starts threads that receives the msg and one that sends with delay handling
#1. Thread that receives the msg from coord server
#2. Thread that handles msg being sent to destination A
#3. Thread that handles msg being sent to destination B
#4. Thread that handles msg being sent to destination C
#5. Thread that handles msg being sent to destination D
thr = receiving(receive_soc)
thr.start()
thr2 = qu_waiter(send_soc, qA, actual_time_list_A, 0)
thr2.start()
thr3 = qu_waiter(send_soc, qB, actual_time_list_B, 1)
thr3.start()
thr4 = qu_waiter(send_soc, qC, actual_time_list_C, 2)
thr4.start()
thr5 = qu_waiter(send_soc, qD, actual_time_list_D, 3)
thr5.start()
thr6 = qu_waiter(send_soc, q_ack, actual_time_list_ack, 4)
thr6.start()



#This thread handles msg that is typed in std input. It puts the msg in each queue so that each one can be handled with the delays
try:
	while 1:
		message = None
		while 1:
			message = raw_input()
			msg = message.split()
			if msg[0] in ['Send', 'delete', 'get', 'insert', 'update', 'delay', 'show-all', 'search']:
				if msg[0] == 'Send' and msg[-1] not in ['A', 'B', 'C', 'D']:			
					print 'destination should be either A, B, C, D'
				else:
					break
			else:
				print 'operation should be either Send, delete, get, insert, update, delay, show-all, search'

		if msg[0] == 'get' and msg[2]  == '2':
			value = main_dict.get(msg[1])
			print 'get(' + str(msg[1]) + ') = ' + str(value)
			continue

		elif msg[0] == 'show-all':
			print main_dict
			continue
		
		elif msg[0] == 'search':
			msend(send_soc, message)
			msg = send_soc.recv(100)
			print msg
			continue

		elif msg[0] == 'Send':
			real_msg = ' '.join(msg[1:-1])
			print 'Sent \"' + real_msg + '\" to ' + str(msg[-1]) + ', system time is ' + str(time.time())
			put_in_qu(message)
			continue
		
		#linear and sequential
		elif msg[0] == 'delete':
			put_in_qu(message + ' ' + str(version[0]))
		elif msg[0] == 'get' and msg[2] == '1':
			put_in_qu(message + ' ' + str(version[0]))
		elif msg[0] == 'insert' and msg[3] == '1':
			put_in_qu(message + ' ' + str(version[0]))
		elif msg[0] == 'update' and msg[3] == '1':
			put_in_qu(message + ' ' + str(version[0]))
		elif msg[0] == 'insert' and msg[3] == '2':
			put_in_qu(message + ' ' + str(version[0]))
		elif msg[0] == 'update' and msg[3] == '2':
			put_in_qu(message + ' ' + str(version[0]))
		else:
			put_in_qu(message + ' ' + str(version[0]) + ' A')
			put_in_qu(message + ' ' + str(version[0]) + ' B')
			put_in_qu(message + ' ' + str(version[0]) + ' C')
			put_in_qu(message + ' ' + str(version[0]) + ' D')
		version[0] = version[0] + 1

except:
    print 'The input is not correct'

#closing the socket and end the program
thr.stopIt = True
thr2.stopIt = True
send_soc.close()
receive_soc.close()
sys.exit()
