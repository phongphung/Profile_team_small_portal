__author__ = 'sunary'


from klout_services.key_manager import KeyManager
from utils import my_connection
import json
from klout import Klout
import datetime
from io import BytesIO


class KloutScore():
    MAX_REQUEST_PER_SECOND = 10

    def __init__(self):
        self.add_key()
        self.con_da0 = my_connection.da0_connection()

    def add_key(self):
        list_key = ['wc8es4285he9vpgzajechs8x',
                    '6wtfjhrx47gacrbxkf8vd7y7',
                    'cw55gzcb7et8ean67v4berda',
                    'ghn3e2yq2jvg76bagvsxacm5',
                    'jy438xjf67dr5qexmyrgxzjy',
                    'f8ghhhsghs264h9gxs6k6acz',
                    'dxadecm8sdxsxyzpdwj4pxfb',
                    'uqp3epz8t6zj5gqhbcq2tc5y',
                    'jxxe46j57anavrauvgyxaabh',
                    '9wppcds3y9ahc5t626gpbsc2',
                    'mnpbyq8r2bufcyyy22h7bge3',
                    'ehqy69d7z2ga34ujesxyhcwf',
                    'qdx27nzasjp5prv63ga4a52d',
                    'buj52xqwyp5ncmv25d9xnhbf',
                    '45ndqfqrm7kxetev65p662xq',
                    'a8guqdn95d5gq3uyzcexevje',
                    'dwmrytj5saqmp8sf6ecxygye',
                    'jxbrt6vfrkcrcenkp5krfmf7',
                    'x4usm9xhaa9pz2xeeu3xedh2',
                    '54fauywk7uukyta5rkvwey76',]

        self.key_manager = KeyManager()
        self.key_manager.add_keys(list_key)

        self.key = Klout(self.key_manager.get_key())

    def update_user(self):
        con_psql1 = my_connection.psql1_connection()
        cur_psql1 = con_psql1.cursor()

        cur_psql1.execute('''
            SELECT object_payload->>'id_str'
            FROM mongo.mg_twitter
        ''')
        cpy = BytesIO()
        now = str(datetime.datetime.now())
        for doc, in cur_psql1:
            cpy.write(str(doc) + '\t' + now + '\t' + now + '\t' + '{}\n')

        cpy.seek(0)
        cur_da0 = self.con_da0.cursor()
        cur_da0.copy_from(cpy, 'user_klout_score', columns=('user_id', 'created_at', 'updated_at', 'payload'))
        self.con_da0.commit()

    def test_score(self):
        # klout_id = self.k.identity.klout(gp = 00000).get('id')
        # klout_id = self.k.identity.klout(screenName = 'v2nhat').get('id')
        klout_id = self.key.identity.klout(tw = 111489227).get('id')
        payload = self.key.user.score(kloutId = klout_id)
        payload['id_klout'] = klout_id
        print payload

    def score(self, updated_at = None):
        cur_da0 = self.con_da0.cursor()
        last_user_id = 0
        threshold = 50000
        while True:
            if updated_at:
                cur_da0.execute('''
                    SELECT user_id
                    FROM twitter.user_klout_score
                    WHERE updated_at < %s
                        AND user_id > %s
                    ORDER BY user_id
                    LIMIT %s
                ''', [updated_at, last_user_id, threshold])
            else:
                cur_da0.execute('''
                    SELECT user_id
                    FROM twitter.user_klout_score
                    WHERE user_id > %s
                    ORDER BY user_id
                    LIMIT %s
                ''', [last_user_id, threshold])
            if cur_da0.rowcount:
                count_get = 0
                for doc, in cur_da0:
                    if doc > last_user_id:
                        last_user_id = doc
                    try:
                        # get 10 kloutscore/key
                        count_get += 1
                        if count_get >= self.MAX_REQUEST_PER_SECOND:
                            count_get = 0
                            self.key = Klout(self.key_manager.get_key())

                        klout_id = self.key.identity.klout(tw = int(doc)).get('id')
                        payload = self.key.user.score(kloutId = klout_id)
                        payload['id_klout'] = klout_id

                        cur2_da0 = self.con_da0.cursor()
                        cur2_da0.execute('''
                            UPDATE twitter.user_klout_score
                            SET payload = %s, updated_at = now()
                            WHERE user_id = %s
                        ''', [json.dumps(payload), int(doc)])
                        self.con_da0.commit()
                    except:
                        cur2_da0 = self.con_da0.cursor()
                        cur2_da0.execute('''
                            UPDATE twitter.user_klout_score
                            SET has_score = false, updated_at = now()
                            WHERE user_id = %s
                        ''', [int(doc)])
                        self.con_da0.commit()
            else:
                break
        self.con_da0.close()

if __name__ == '__main__':
    klout_score = KloutScore()
    klout_score.score()