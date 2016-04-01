import sys
import threading
import Queue
import time
from threading import Timer
#threadlist
thread_dict = {}

#start time of system
start_time = time.time()

main_queue = Queue.Queue(maxsize = 1)

option = 0

#argv for program
if len(sys.argv) >= 4:
    cs_int = sys.argv[1]
    next_req = sys.argv[2]
    tot_exec_time = sys.argv[3]
    if(len(sys.argv) == 5):
    	option = sys.argv[4]


else:
    print "Need more information"

#milliseconds
option = int(option)
c = float(cs_int)
cs_int = c / 1000
n = float(next_req)
next_req = n / 1000   





#release information recieved 
def release_receive(tid, check):
	if(thread_dict[tid].request.empty() != True):
		p = thread_dict[tid].request.get()
		if check == 1:
			print time.time(), tid , p, " release receive"
		thread_dict[p].receive.put(tid)
		thread_dict[tid].voted = True

	else:
		thread_dict[tid].voted = False

#request receive from other node
def request_receive(tid, check):
	if(thread_dict[tid].state != 2 and thread_dict[tid].voted == False):
		reply = thread_dict[tid].request.get() 
		if check == 1:
			print time.time(), tid, reply, " request receive"
		thread_dict[reply].receive.put(tid)
		thread_dict[tid].voted = True

#control the messages when it is in cs 
class message_thread(threading.Thread):
	def __init__(self, tid, op):
		threading.Thread.__init__(self)
		self.tid = tid
		self.done = False
		self.op = op
		

	def run(self):
		while not self.done:
			#global thread_dict
			if((thread_dict[self.tid].got_request) == 1 ):
				request_receive(self.tid, self.op)
				thread_dict[self.tid].got_request = 0

			if((thread_dict[self.tid].got_release) == 1 ):
				release_receive(self.tid, self.op)
				thread_dict[self.tid].got_release = 0


#9 threads 
#state explanation
# 0 = Released
# 1 = Wanted
# 2 = HELD

class mutex_thread(threading.Thread):
	def __init__(self, thread_Id, lock, op):
		threading.Thread.__init__(self)
		self.state = 0
		self.voted = False
		self.op = op
		self.done = False
		self.thread_Id = thread_Id

		self.voting_set = []
		self.request = Queue.Queue(maxsize = 0)
		self.receive = Queue.Queue(maxsize = 0)

		self.held = Queue.Queue(maxsize = 0)
		self.Release = Queue.Queue(maxsize = 0)
		self.rel_list = []

		self.lock = lock


		self.subthread = message_thread(self.thread_Id, self.op)
		self.got_request = 0
		self.got_release = 0
		self.request_done = 0

	def run(self):
		while not self.done:
			if(self.request_done == 5):
				self.state = 2
				self.request_done = 0
			#cs
			if(self.state == 2):
				self.lock.acquire()
				main_queue.put((self.thread_Id, self.voting_set))
				#in cs
				delay = main_queue.get(timeout = cs_int)
				voting_list = []
				for i in delay[1]:
					voting_list.append(i.thread_Id)
				
				#time to enter the cs

				print time.time(), " ", delay[0], " ", voting_list
				
				time.sleep(cs_int)

				self.lock.release()
				
				thread_dict[self.thread_Id].exit_cs()

			#get voting
			if(self.receive.empty() != True):
				p = self.receive.get()
				if self.op == 1:
					print self.thread_Id, p, "voting"
				self.request_done = self.request_done + 1

			

	#send request to voting set
	def request_send(self):
		self.state = 1
		
		for thread_request in self.voting_set:
			(thread_request.request).put(self.thread_Id)
			thread_request.got_request = 1
			thread_request.rel_list[self.thread_Id] = False

	#exit critical section
	def exit_cs(self):
		self.state = 0
		for rel_thread in self.voting_set:
			rel_thread.rel_list[self.thread_Id] = True
			rel_thread.got_release = 1

		self.Release.put("releasing")
		rel = self.Release.get(timeout = float(next_req))
		time.sleep(next_req)

		thread_dict[self.thread_Id].request_send()





#main_thread
if __name__ == "__main__":


	lock = threading.Lock()



	for i in range(0, 9):
		thread_dict[i] = mutex_thread(i, lock, option)


	#updating voting set
	thread_dict[0].voting_set = [thread_dict[0], thread_dict[1], thread_dict[2], thread_dict[3], thread_dict[6]]
	thread_dict[1].voting_set = [thread_dict[0],thread_dict[1], thread_dict[2], thread_dict[4], thread_dict[7]]
	thread_dict[2].voting_set = [thread_dict[0],thread_dict[1], thread_dict[2], thread_dict[5], thread_dict[8]]
	thread_dict[3].voting_set = [thread_dict[0], thread_dict[3], thread_dict[4],thread_dict[5], thread_dict[6]]
	thread_dict[4].voting_set = [thread_dict[1], thread_dict[3], thread_dict[4],thread_dict[5], thread_dict[7]]
	thread_dict[5].voting_set = [thread_dict[2], thread_dict[3], thread_dict[4], thread_dict[5],thread_dict[8]]
	thread_dict[6].voting_set = [thread_dict[0], thread_dict[3], thread_dict[6], thread_dict[7], thread_dict[8]]
	thread_dict[7].voting_set = [thread_dict[1], thread_dict[4], thread_dict[6], thread_dict[7], thread_dict[8]]
	thread_dict[8].voting_set = [thread_dict[2],thread_dict[5], thread_dict[6], thread_dict[7], thread_dict[8]]


	#start the progrma
	for i in range(0, 9):
		thread_dict[i].rel_list = [False, False, False, False, False, False, False, False, False]

	for i in range(0,9):
		thread_dict[i].start()

	for i in range(0, 9):
		(thread_dict[i].subthread).start()


	for i in range(0,9) :
		thread_dict[i].request_send()

	#sleep main thread
	time.sleep(float(tot_exec_time));


	

	for i in range(0,9):
		(thread_dict[i].subthread).done = True
		(thread_dict[i].subthread).join()
		thread_dict[i].done = True
		thread_dict[i].join()


	

