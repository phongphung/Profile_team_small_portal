__author__ = 'sunary'


from automation.kpi_from_db import KPIFromDb
from utils import my_connection


class PublisherSnapShot():

    def __init__(self):
        pass

    def process(self):
        con_psql0 = my_connection.psql0_connection()

        cur_psql0 = con_psql0.cursor()
        cur_psql0.execute('''
            WITH pub AS (
                SELECT
                    object_id,
                    json_array_elements(object_payload->'categories') AS category
                  FROM mongo.mg_publisher
                  WHERE object_payload->>'categories' != '[]'
            ), pub_bd AS (
                SELECT
                    (json_array_elements(object_payload->'categories'))->>'_id' AS category_mongo_id,
                    COUNT(*) AS publisher_count,
                    COUNT(ps5.sns_id) AS tw_count,
                    COUNT(ps3.sns_id) AS li_count,
                    COUNT(ps4.sns_id) AS news_count,
                    (COUNT(ps1.sns_id) + COUNT(ps2.sns_id)) AS other_count,
                    SUM(CASE object_payload->>'countryCode' WHEN 'AU' THEN 1 ELSE 0 END) AS au_count,
                    SUM(CASE object_payload->>'countryCode' WHEN 'CA' THEN 1 ELSE 0 END) AS ca_count,
                    SUM(CASE object_payload->>'countryCode' WHEN 'CH' THEN 1 ELSE 0 END) AS ch_count,
                    SUM(CASE object_payload->>'countryCode' WHEN 'CN' THEN 1 ELSE 0 END) AS cn_count,
                    SUM(CASE object_payload->>'countryCode' WHEN 'DE' THEN 1 ELSE 0 END) AS de_count,
                    SUM(CASE object_payload->>'countryCode' WHEN 'GB' THEN 1 ELSE 0 END) AS gb_count,
                    SUM(CASE object_payload->>'countryCode' WHEN 'HK' THEN 1 ELSE 0 END) AS hk_count,
                    SUM(CASE object_payload->>'countryCode' WHEN 'IN' THEN 1 ELSE 0 END) AS in_count,
                    SUM(CASE object_payload->>'countryCode' WHEN 'SA' THEN 1 ELSE 0 END) AS sa_count,
                    SUM(CASE object_payload->>'countryCode' WHEN 'SG' THEN 1 ELSE 0 END) AS sg_count,
                    SUM(CASE object_payload->>'countryCode' WHEN 'US' THEN 1 ELSE 0 END) AS us_count
                  FROM mongo.mg_publisher p
                  LEFT JOIN mongo.mg_publisher_sns ps1 ON (p.object_id = ps1.publisher_mongo_id AND ps1.sns_name = 'fb')
                  LEFT JOIN mongo.mg_publisher_sns ps2 ON (p.object_id = ps2.publisher_mongo_id AND ps2.sns_name = 'gp')
                  LEFT JOIN mongo.mg_publisher_sns ps3 ON (p.object_id = ps3.publisher_mongo_id AND ps3.sns_name = 'li')
                  LEFT JOIN mongo.mg_publisher_sns ps4 ON (p.object_id = ps4.publisher_mongo_id AND ps4.sns_name = 'news')
                  LEFT JOIN mongo.mg_publisher_sns ps5 ON (p.object_id = ps5.publisher_mongo_id AND ps5.sns_name = 'tw')
                  WHERE (object_payload->'categories') IS NOT NULL
                    AND object_payload->>'categories' != '[]'
                    AND EXISTS (
                        SELECT NULL
                          FROM pub
                          WHERE pub.object_id = p.object_id
                            AND pub.category->>'_id' IN (
                                '52d4bd98e4b08a17cb0aa9bf', -- P
                                '52d4bd98e4b08a17cb0aa9c0'  -- O
                            )
                    )
                  GROUP BY
                    category_mongo_id
            )
            SELECT
                get_category_name_path_by_mongo_id(p.category_mongo_id, '>') AS category_name_path,
                p.*
              FROM pub_bd p
              ORDER BY
                category_name_path
        ''')

        size_m = 42
        size_n = 16
        self.list_value = []
        len_doc = 0
        for doc in cur_psql0:
            for i in range(2, 2 + size_n):
                self.list_value.append(doc[i])
            len_doc += 1
            if len_doc >= size_m:
                break

        con_psql0.close()

    def insert_db(self):
        con_dev = my_connection.dev_connection('nhat')

        list_id = KPIFromDb.ID_PUBLISHER_SNAPSHOT

        cur_dev = con_dev.cursor()
        for value in self.list_value:
            cur_dev.execute('''
                INSERT INTO public.kpi_report(id, created_at, value)
                VALUES(%s, now(), %s)
            ''', [list_id, value])
            con_dev.commit()

        con_dev.close()


if __name__ == '__main__':
    publisher_snapshot = PublisherSnapShot()
    publisher_snapshot.process()
    publisher_snapshot.insert_db()
