__author__ = 'sunary'


import datetime
import time


class Key():
    '''
    Keys manager
    '''
    MAX_REQUEST_PER_DAY = 2000

    def __init__(self, str_key):
        self.str_key = str_key
        self.reset()

    def reset(self):
        self.count_res = 0
        self.time_to_reset = datetime.datetime.now() + datetime.timedelta(days= 1)

    def get_key(self):
        if self.count_res < self.MAX_REQUEST_PER_DAY:
            self.count_res += 1
            return self.str_key
        elif datetime.datetime.now() > self.time_to_reset:
            self.reset()
            self.count_res += 1
            return self.str_key
        else:
            return ''

class KeyManager():
    '''
    Keys manager by max-request per day
    '''
    def __init__(self):
        self.rotate_key = 0
        self.key = []

    def add_key(self, str_key):
        self.key.append(Key(str_key))

    def add_keys(self, str_keys):
        for str_key in str_keys:
            self.add_key(str_key)

    def get_key(self):
        while True:
            str_key = self.key[self.rotate_key].get_key()
            self.rotate_key = (self.rotate_key + 1)%len(self.key)
            if str_key:
                return str_key
            time.sleep(10)

if __name__ == '__main__':
    key_manager = KeyManager()
    list_key = ['wc8es4285he9vpgzajechs8x',
                '6wtfjhrx47gacrbxkf8vd7y7',
                'cw55gzcb7et8ean67v4berda',
                'ghn3e2yq2jvg76bagvsxacm5',
                'jy438xjf67dr5qexmyrgxzjy',
                'f8ghhhsghs264h9gxs6k6acz',
                'dxadecm8sdxsxyzpdwj4pxfb',
                'uqp3epz8t6zj5gqhbcq2tc5y',]
    key_manager.add_keys(list_key)

    for i in range(100):
        print key_manager.get_key()