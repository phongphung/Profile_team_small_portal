__author__ = 'sunary'


from utils import my_connection
from automation.kpi_from_db import KPIFromDb
from redis import StrictRedis


class DailyReadRedis():

    def __init__(self):
        self.redis_dev = StrictRedis(host='10.0.0.240', port=6379)
        self.time_expire = 86400*30

    def number_change_profile(self):
        self.list_id = [KPIFromDb.ID_NUM_CHANGE_DES, KPIFromDb.ID_NUM_CHANGE_AVATAR, KPIFromDb.ID_NUM_CHANGE_LOC, KPIFromDb.ID_NUM_CHANGE_URL, KPIFromDb.ID_NUM_CHANGE_ANY]
        self.nums_change = [0]*len(self.list_id)

        for i in range(len(self.list_id)):
            self.nums_change[i] = self.redis_dev.get(self.list_id[i])
            try:
                self.nums_change[i] = int(self.nums_change[i]) if self.nums_change[i] else 0
            except:
                self.nums_change[i] = 0

            self.redis_dev.set(self.list_id[i], 0)
            self.redis_dev.expire(self.list_id[i], self.time_expire)

    def insert_db(self):
        con_dev = my_connection.dev_connection('nhat')

        list_id = self.list_id
        list_value = self.nums_change

        cur_dev = con_dev.cursor()
        for i in range(len(list_id)):
            cur_dev.execute('''
                INSERT INTO public.kpi_report(id, created_at, value)
                VALUES(%s, now(), %s)
            ''', [list_id[i], list_value[i]])
            con_dev.commit()

        con_dev.close()


if __name__ == '__main__':
    daily_read = DailyReadRedis()
    daily_read.number_change_profile()
    daily_read.insert_db()
