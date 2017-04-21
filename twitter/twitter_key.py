__author__ = 'sunary'


from datetime import datetime, timedelta
import yaml
import time


class TwitterKey():

    SCREEN_TIME = 15*60
    _id_key = 0
    _available_at = []
    _keys = []
    _new_loop = True

    def __init__(self, twitter_key_path):
        with open(twitter_key_path, 'r')as fo:
            self._keys = yaml.load(fo)

        self._available_at = [datetime.now() for _ in range(len(self._keys))]

    def get(self):
        '''
        get new twitter key, and set next available previous key
        '''

        if not self._new_loop:
            self._available_at[self._id_key - 1] = datetime.now() + timedelta(seconds=self.SCREEN_TIME)
        else:
            self._new_loop = False

        self._id_key = self._id_key if self._id_key < len(self._keys) else 0

        if self._available_at[self._id_key] >= datetime.now():
            time.sleep((self._available_at[self._id_key] - datetime.now() + timedelta(seconds=1)).total_seconds())

        self._id_key += 1
        return self._keys[self._id_key - 1]

    def report(self):
        self._id_key -= 1
        del self._keys[self._id_key]
        del self._available_at[self._id_key]
        return len(self._keys)


if __name__ == '__main__':
    key_engine = TwitterKey('twitter_key.yml')
    print key_engine.get()
    time.sleep(10*60)
    print key_engine.get()
    print key_engine.get()


