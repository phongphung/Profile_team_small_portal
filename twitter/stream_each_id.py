__author__ = 'sunary'


from time import sleep
from multiprocessing import Process


class StreamEachId():
    '''
    Each stream listen each id
    '''
    def __init__(self):
        self.list_process = []

    def run(self):
        list_ids = [i for i in range(10)]

        for id in list_ids:
            self.list_process.append({'process': Process(target=self.listener, args=(id,)), 'user_id': id})
            self.list_process[-1]['process'].start()
            sleep(1)

        for p in self.list_process:
            if p['user_id'] % 3 == 0:
                p['process'].terminate()

    def add_user_id(self, user_id):
        self.list_process.append({'process': Process(target=self.listener, args=(user_id,)), 'user_id': user_id})
        self.list_process[-1]['process'].start()

    def listener(self, user_id):
        print 'start process: %s' % user_id
        sleep(30)
        print 'end process: %s' % user_id

    def terminate(self, user_id):
        for p in self.list_process:
            if p['user_id'] == user_id:
                p['process'].terminate()
                p['process'].join()

if __name__ == '__main__':
    stream_each_id = StreamEachId()
    stream_each_id.run()
    stream_each_id.add_user_id(25)
    stream_each_id.terminate(4)