__author__ = 'sunary'


import os
import thread
import psutil
import atexit
from time import sleep
from multiprocessing import Process, Queue


class StreamManyIds():
    '''
    Each stream listen many ids
    Kill child process if parent are die
    '''
    def __init__(self):
        self.list_process = []
        self.chunk_size = 8
        self.num_process = 0
        # self.queue = Queue()
        self.last_process_chunk_id = []
        atexit.register(self.kill_process)

    def run(self):
        list_ids = [i for i in range(23)]

        num_id_in_chunk = 0
        chunk_id = []
        for id in list_ids:
            chunk_id.append(id)
            num_id_in_chunk += 1
            if num_id_in_chunk >= self.chunk_size:
                self.list_process.append({'process': Process(target=self.listener, args=(chunk_id, os.getpid(),)), 'chunk_id': self.num_process})
                self.list_process[-1]['process'].start()
                self.list_process[-1]['pid'] = self.list_process[-1]['process'].pid
                self.num_process += 1
                num_id_in_chunk = 0
                self.last_process_chunk_id = chunk_id
                chunk_id = []
                sleep(1)

        if len(chunk_id) > 0:
            self.list_process.append({'process': Process(target=self.listener, args=(chunk_id, os.getpid(),)), 'chunk_id': self.num_process})
            self.list_process[-1]['process'].start()
            self.list_process[-1]['pid'] = self.list_process[-1]['process'].pid
            self.num_process += 1
            self.last_process_chunk_id = chunk_id
            sleep(1)

        thread.start_new_thread(self.thread_check_child_process())

    def thread_check_child_process(self):
        '''
        restart child process if it's die
        '''
        while True:
            for p in self.list_process:
                if not p['process'].is_alive():
                    p['process'] = Process(target=self.listener, args=([1, 1, 1], os.getpid(),))
                    p['process'].start()
                    p['pid'] = p['process'].pid

            sleep(10)

    def add_user_id(self, user_id):
        # self.last_process_chunk_id = self.queue.get()
        if len(self.last_process_chunk_id) == self.chunk_size:
            self.last_process_chunk_id = [user_id]
            self.list_process.append({'process': Process(target=self.listener, args=(self.last_process_chunk_id, os.getpid(),)), 'chunk_id': self.num_process})
            self.list_process[-1]['process'].start()
            self.list_process[-1]['pid'] = self.list_process[-1]['process'].pid
            self.num_process += 1
            sleep(1)
        else:
            self.last_process_chunk_id.append(user_id)
            self.list_process[-1]['process'].terminate()
            self.list_process[-1]['process'].join()
            self.list_process[-1]['process'] =  Process(target=self.listener, args=(self.last_process_chunk_id, os.getpid(),))
            self.list_process[-1]['process'].start()
            self.list_process[-1]['pid'] = self.list_process[-1]['process'].pid
            sleep(1)

    def listener(self, user_ids, ppid):
        # self.queue.put(user_ids)
        print 'start process: %s' % user_ids
        thread.start_new_thread(self.thread_check_exist_ppid, (ppid,))
        while True:
            pass

    def thread_check_exist_ppid(self, ppid):
        '''
        check exist parent process id every 10s,
        if True itself exit
        '''
        while True:
            # if not psutil.pid_exists(ppid):
            if os.getppid() != ppid:
                os._exit(os.getpid())

            sleep(10)

    def terminate(self, id_process):
        for p in self.list_process:
            if p['chunk_id'] == id_process:
                p['process'].terminate()
                p['process'].join()

    def kill_process(self):
        for p in self.list_process:
            try:
                p.terminate()
                p.join()
            except:
                pass

if __name__ == '__main__':
    stream_many_ids = StreamManyIds()
    stream_many_ids.run()
    stream_many_ids.add_user_id(23)
    stream_many_ids.add_user_id(24)
    stream_many_ids.add_user_id(25)
    stream_many_ids.terminate(1)