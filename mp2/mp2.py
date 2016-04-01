import socket
import threading
import time
import Queue
import random
import sys
import math
import thread
import collections

#Assumed 8-bit as specified on the instruction
max_bit = 8
thread_dict = {}

#count
c_queue = Queue.Queue(maxsize=0)

#class for output data - saving output to file
class Logger(object):
    def __init__(self, filename="Default.log"):
        self.terminal = sys.stdout
        self.log = open(filename, "w")

    def write(self, message):
        self.terminal.write(message)
        self.log.write(message)

#saving all stdout to out.txt when it has -g argv
if(len(sys.argv) > 1):
    if sys.argv[1] == "-g" and sys.argv[2] != " ":
        sys.stdout = Logger(sys.argv[2])

#helper function(int, int, int) This include left and right
def check_interval(value, left, right):
    while left < 0:
	left = left + 256

    while right < 0:
	right = right + 256
	
    if left == right:
	if left == value:
	    return True
	else:
	    return False

    if left < right: #normal case
	if left <= value and value <= right:
	    return True
	else:
	    return False

    else: #interval has 0 between them(left > right)
	if right < value and value < left:
	    return False	
	else:
	    return True

#node class thread
class node(threading.Thread):
    def __init__(self, node_id, m):
        threading.Thread.__init__(self)
	#bit
	self.m = m	

	#flag for leave
	self.leave_check = False

	#finger table
	self.start_list = []
        self.interval_list = []
        self.successor_list = [-1] * self.m
	
	#Stored as thread not as int
	self.predecessor = None
	
        self.node_keys = []
        self.node_id = node_id

        #make start list
        for i in range(0, self.m):
            (self.start_list).append((self.node_id + 2**i) % 2**self.m)
        
        #make interval list
        for i in range(0, self.m):
            if(i < self.m-1):
                (self.interval_list).append((self.start_list[i], self.start_list[i+1]))
            if(i == self.m-1):
                (self.interval_list).append((self.start_list[i], self.node_id))

    #running 
    def run(self):
        while not self.leave_check:
            t = 0
    
    #for node 0
    def first(self):
	for i in range(0, 256):
    	    self.node_keys.append(i)
	for i in range(0, self.m):
	    self.successor_list[i] = 0
	self.predecessor = self
	count = 0
	c_queue.put(count)

    #successor()
    def successor(self):
	return thread_dict[self.successor_list[0]]

    #algorithm for find successor(From the Chord paper)
    def find_successor(self, key): #return int
    	n_prime = self.find_predecessor(key)
	return n_prime.successor().node_id

    def find_predecessor(self, key): #return object
    	n_prime = self
    	while check_interval(key, n_prime.node_id + 1, n_prime.successor().node_id) == False:
	    n_prime = n_prime.closest_preceding_finger(key) #2
	    #add count
	    count = c_queue.get()
	    count = count + 2
	    c_queue.put(count)
    	return n_prime

    def closest_preceding_finger(self, key):
    	for i in range(self.m-1, -1, -1):
	    if check_interval(self.successor_list[i], self.node_id+1, key-1) == True:
	    	return thread_dict[self.successor_list[i]]
    	return self

    #function for join(From the Chord paper with fix)
    def join_node(self, n_prime): #we assume that n_prime is always 0 and 0 is always there    
	self.init_finger_table(n_prime)
    	self.update_others()

    def init_finger_table(self, n_prime):
    	self.successor_list[0] = n_prime.find_successor(self.start_list[0]) #2
	self.predecessor = self.successor().predecessor #2
	self.successor().predecessor = self #2
	#add count
	count = c_queue.get()
	count = count + 6
	c_queue.put(count)
	
	#move key when join the node
	self.node_keys = [item for item in self.successor().node_keys if item != 0 and item <= self.node_id] #2
	succ_keys = [item for item in self.successor().node_keys if item > self.node_id or item == self.successor().node_id]
	self.successor().node_keys = succ_keys
	#add count
	count = c_queue.get()
	count = count + 2
	c_queue.put(count)

	for i in range(0, self.m-1):
	    if check_interval(self.start_list[i+1], self.node_id, self.successor_list[i]-1) == True:
		self.successor_list[i+1] = self.successor_list[i]
	    else:
		self.successor_list[i+1] = n_prime.find_successor(self.start_list[i+1]) #2
		#add count
		count = c_queue.get()
		count = count + 2
		c_queue.put(count)
		
		if check_interval(self.successor_list[i+1], self.start_list[i+1], self.node_id) == False:
		    self.successor_list[i+1] = self.node_id

    def update_others(self):
	for i in range(0, self.m):
	    #check for negative value
	    num = self.node_id - 2**i + 1
	    while num < 0:
		num = num + 256
	    
	    p = self.find_predecessor(num)
	    p.update_finger_table(self.node_id, i) #1
	
	    count = c_queue.get()
	    count = count + 1
	    c_queue.put(count)

    def update_finger_table(self, s, i):
	if s == self.node_id:
	    return
	if check_interval(s, self.node_id+1, self.successor_list[i]) == True:
	    self.successor_list[i] = s
	    p = self.predecessor
	    p.update_finger_table(s, i) #1
	    
	    count = c_queue.get()
	    count = count + 1
	    c_queue.put(count)

    #function for leave(From the Chord paper with fix)
    def leave(self):
	if self == self.successor(): #when only node 0 left(Assume it never happens)
	    return
	
    	#move keys when leave
    	temp = self.node_keys
	self.successor().node_keys = sorted(temp + self.successor().node_keys)
    	
	self.successor().predecessor = self.predecessor
	for i in range(0, self.m):
	    #check for negative value
	    num = self.node_id - 2**i + 1
	    while num < 0:
		num = num + 256
	    
	    p = self.find_predecessor(num)
	    p.remove_node(self.node_id, i, self.successor().node_id);
	self.leave_check = True
	
    def remove_node(self, n, i, repl):
	if self.successor_list[i] == n:
	    self.successor_list[i] = repl
	    self.predecessor.remove_node(n, i, repl)

#initialize node 0 (key from 0 to 255)
node_zero = node(0, max_bit)
node_zero.start()
node_zero.first()
thread_dict[0] = node_zero

#command for the mp2
#(join p), (find p k), (leave p), (show p), (show all)
while 1:
    message = None
    exit_flag = 0
    while 1: #check for the valid input
	message = raw_input()
	msg = message.split()
	if(msg[0] == 'exit'):
	    exit_flag = 1
	    break
	if msg[0] in ['join', 'find', 'leave', 'show']:
	    if msg[0] == 'find':
		if len(msg) == 3:
		    break
		else:
		    print 'usage: find p k'
	    elif len(msg) != 2:
		print 'usage: ([join, leave, show] p) or (show all)'
	    else:
		break
	else:
	    print 'operation should be either join, find, leave, show (all lower case)'
    #message is validated at this point

    if exit_flag == 1:
	break

    #show all
    if msg[0] == 'show' and msg[1] == 'all':
        for thread in sorted(thread_dict.keys()):
            print thread_dict[thread].node_id, ' '.join(map(str, sorted(thread_dict[thread].node_keys))) 
        continue

    #show f(for debugging)
    if msg[0] == 'show' and msg[1] == 'f':
        for thread in thread_dict:
            print 'Node:', str(thread_dict[thread].node_id) + '\n' + str(thread_dict[thread].successor_list)
        continue

    #show q(for counting)
    if msg[0] == 'show' and msg[1] == 'q':
        count = c_queue.get()
	print count
	c_queue.put(count)
        continue
    
    #if not show all, p should be int
    p = None
    try:
	p = int(msg[1])
    except:
	print 'p should be int'
	continue

    if p < 0 or p > 255:
	print 'p should be 0 ~ 255'
	continue

    #join command
    if msg[0] == 'join':
	try: #if node already exist
	    if thread_dict[p] != None:
		print 'Node', p, 'already exist'
		continue
    	except:
            new_node = node(p, max_bit)
	    thread_dict[p] = new_node
	    new_node.start()
	    new_node.join_node(thread_dict[0])

    #find command
    elif msg[0] == 'find':
	k = None
	p_node = None

	try:
	    k = int(msg[2])
	    if k < 0 or k > 255:
		print 'k should be 0 ~ 255'
		continue

    	except:
	    print 'k should be int'
	    continue        
	
	try:
	    p_node = thread_dict[p]
	except:
            print 'there is no ', p, 'node'
            continue
    	
    	print 'key', k, 'is in', thread_dict[p].find_successor(k)

    #leave command
    elif msg[0] == 'leave':
	try: #if node exist
	    if thread_dict[p] != None:
	        thread_dict[p].leave()
		del thread_dict[p]
    	except:
	    print 'Node', p, 'does not exist'
	
    #show command
    elif msg[0] == 'show':
	p_node = None
        try:
	    p_node = thread_dict[p]
	except:
            print 'there is no', p, 'node'
            continue

        print thread_dict[p].node_id, ' '.join(map(str, sorted(thread_dict[p].node_keys)))

for i in thread_dict.keys():
    thread_dict[i].leave_check = True
    thread_dict[i].join()

