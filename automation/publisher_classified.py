__author__ = 'sunary'


from utils import my_datetime, my_connection
from automation.kpi_from_db import KPIFromDb


class PublisherClassified():

    def __init__(self):
        self.con_dev = my_connection.dev_connection('nhat')
        self.size_m = len(KPIFromDb.CATEGORIES)
        self.size_n = len(KPIFromDb.SELECTED_ISOCODE)

    def process(self):
        '''
        cal last 7 days and add with old data
        '''
        isocode = {}
        for i in range(len(KPIFromDb.SELECTED_ISOCODE)):
            isocode[KPIFromDb.SELECTED_ISOCODE[i]] = i

        categories = {}
        for i in range(len(KPIFromDb.CATEGORIES)):
            categories[KPIFromDb.CATEGORIES[i]] = i

        self.publisher_location = [[0]*self.size_m for _ in range(self.size_n)]

        con_da0 = my_connection.da0_connection()
        cur_da0 = con_da0.cursor()
        # unnest occupations
        cur_da0.execute('''
            SELECT DISTINCT iso.isocode,
                unnest(uc.occupations),
                count(*)
            FROM user_classified_data uc
                INNER JOIN user_isocode iso ON(iso.user_id = uc.user_id)
            WHERE iso.isocode IN %s
                AND uc.occupations != '{}'
                AND EXISTS(
                    SELECT NULL
                    FROM user_tracking ut
                    WHERE ut.user_id = uc.user_id
                        AND ut.is_classified is not null
                        AND ut.updated_at > %s
                )
            GROUP BY iso.isocode, uc.occupations
        ''', [KPIFromDb.SELECTED_ISOCODE, my_datetime.date_previous_days(7)])

        for doc in cur_da0:
            self.publisher_location[isocode[doc[0]]][categories[doc[1]]] = doc[2]

        #unnest func_roles
        cur_da0.execute('''
            SELECT DISTINCT iso.isocode,
                unnest(uc.func_roles),
                count(*)
            FROM user_classified_data uc
                INNER JOIN user_isocode iso ON(iso.user_id = uc.user_id)
            WHERE iso.isocode IN %s
                AND uc.func_roles != '{}'
                AND EXISTS(
                    SELECT NULL
                    FROM user_tracking ut
                    WHERE ut.user_id = uc.user_id
                        AND ut.is_classified is not null
                        AND ut.updated_at > %s
                )
            GROUP BY iso.isocode, uc.func_roles
        ''', [KPIFromDb.SELECTED_ISOCODE, my_datetime.date_previous_days(7)])

        for doc in cur_da0:
            self.publisher_location[isocode[doc[0]]][categories[doc[1]]] = doc[2]

        # cal remain isocode
        '''
            SELECT unnest(uc.occupations),
                count(*)
            FROM user_classified_data uc
                INNER JOIN user_isocode iso ON(iso.user_id = uc.user_id)
            WHERE iso.isocode != ''
                AND iso.isocode NOT IN %s
                AND uc.occupations != '{}'
                AND EXISTS(
                    SELECT NULL
                    FROM user_tracking ut
                    WHERE ut.user_id = uc.user_id
                        AND ut.is_classified is not null
                        AND ut.updated_at > %s
                )
            GROUP BY uc.occupations
        '''

        # cal missing isocode
        '''
            SELECT unnest(uc.occupations),
                count(*)
            FROM user_classified_data uc
                INNER JOIN user_isocode iso ON(iso.user_id = uc.user_id)
            WHERE iso.isocode = ''
                AND uc.occupations != '{}'
                AND EXISTS(
                    SELECT NULL
                    FROM user_tracking ut
                    WHERE ut.user_id = uc.user_id
                        AND ut.is_classified is not null
                        AND ut.updated_at > %s
                )
            GROUP BY uc.occupations
        '''

        # cal don't exist in isocode
        '''
            SELECT unnest(uc.occupations),
                count(*)
            FROM user_classified_data uc
            WHERE uc.occupations != '{}'
                AND EXISTS(
                    SELECT NULL
                    FROM user_tracking ut
                    WHERE ut.user_id = uc.user_id
                        AND ut.is_classified is not null
                        AND ut.updated_at > %s
                )
                AND NOT EXISTS(
                    SELECT NULL
                    FROM user_isocode iso
                    WHERE iso.user_id = uc.user_id
                )
            GROUP BY uc.occupations
        '''

    def read_old_data(self):
        list_id = KPIFromDb.ID_PUBLISHER_QUALIFIED_ISOCODE

        cur_dev = self.con_dev.cursor()
        cur_dev.execute('''
            SELECT value
            FROM public.kpi_report
            WHERE id = %s
            ORDER BY created_at DESC
            LIMIT %s
        ''', [list_id, self.size_m*self.size_n])

        list_result = [[0]*self.size_n for _ in range(self.size_m)]

        count_result = self.size_m*self.size_n - 1
        for doc in cur_dev:
            list_result[count_result%self.size_m][count_result/self.size_m] = doc[0]
            count_result -= 1

        for i in range(self.size_m):
            for j in range(self.size_n):
                self.publisher_location[i][j] += list_result[j][i]

    def insert_db(self):
        self.read_old_data()

        list_id = KPIFromDb.ID_PUBLISHER_QUALIFIED_ISOCODE

        cur_dev = self.con_dev.cursor()
        for by_isocode in self.publisher_location:
            for value in by_isocode:
                cur_dev.execute('''
                    INSERT INTO public.kpi_report(id, created_at, value)
                    VALUES(%s, now(), %s)
                ''', [list_id, value])
                self.con_dev.commit()

        self.con_dev.close()

if __name__ == '__main__':
    publisher_classified = PublisherClassified()
    publisher_classified.process()
    publisher_classified.insert_db()
