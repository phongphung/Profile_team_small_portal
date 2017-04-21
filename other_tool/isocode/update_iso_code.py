__author__ = 'sunary'


from utils import my_connection, my_helper
import os
import pandas as pd


path = os.path.dirname(os.path.abspath(__file__))

class UpdateIsoCode():

    def __init__(self):
        #run in client
        # self.eval_model = EvalLocation(graph_file='/'.join([path, 'iso_location_pgm.pkl']))

        #run in server
        self.con_da0 = my_connection.da0_connection()
        self.logger = my_helper.init_logger(self.__class__.__name__)

    #run in server
    def get_id(self):
        cur_da0 = self.con_da0.cursor()

        cur_da0.execute('''
            SELECT user_id, payload->>'location'
            FROM tw_user
            WHERE NOT EXISTS (
                SELECT user_id
                FROM user_isocode
                WHERE user_isocode.user_id = tw_user.user_id)
                AND payload->>'location' != ''
            LIMIT 1000000
        ''')
        self.res = cur_da0.fetchall()
        self.logger.info('Done: get it need update')

    #can not run in server
    def update_iso_code(self):
        cur_da0 = self.con_da0.cursor()
        for id in self.res:
            iso_code = self.eval_model.eval(id[1])
            iso_code = iso_code if iso_code else ''
            cur_da0.execute('''
                INSERT INTO user_isocode(user_id, iso_code)
                VALUES(%s, %s)
            ''', (id[0], iso_code))

        self.logger.info('Done: insert user_id and iso code')

    #run in server
    def save_id_to_csv(self):
        dict_data = {'id':[], 'location': []}
        for id in self.res:
            dict_data['id'].append(id[0])
            dict_data['location'].append(id[1])

        dataframe = pd.DataFrame(data=dict_data, columns=['id', 'location'])
        dataframe.to_csv('list_need_convert_isocode.csv', index=False)

    #run in client
    def convert_to_isocode(self):
        pd_file = pd.read_csv('/home/sunary/data_report/list_need_convert_isocode.csv')

        for i in range(len(pd_file['location'])):
            iso_code = self.eval_model.eval(str(pd_file['location'][i]))
            iso_code = iso_code if iso_code else ''
            pd_file['location'][i] = iso_code

        pd_file.to_csv('list_isocode.csv', index=False)

    #run in server
    def update_isocode_form_csv(self):
        pd_file = pd.read_csv('list_isocode.csv')

        cur_da0 = self.con_da0.cursor()
        for i in range(len(pd_file['id'])):
            cur_da0.execute('''
                INSERT INTO user_isocode(user_id, iso_code)
                VALUES(%s, %s)
            ''', (int(pd_file['id'][i]), pd_file['location'][i]))
            self.con_da0.commit()

        self.logger.info('Done: insert user_id and iso code')

    def close_connection(self):
        self.con_da0.close()

if __name__ == '__main__':
    update_iso_code = UpdateIsoCode()
    # update_iso_code.convert_to_isocode()
    # update_iso_code.update_iso_code()
    update_iso_code.update_isocode_form_csv()
    update_iso_code.close_connection()
