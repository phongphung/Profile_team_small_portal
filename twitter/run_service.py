__author__ = 'sunary'


import os
from utils import my_helper, my_connection as conn
from twitter.twitter_services import TwitterService
from queue.my_queue import Queue
import pandas as pd


class RunService():

    def __init__(self):
        pass

    def crawl_following_follower(self):
        twitter_service = TwitterService('/'.join([os.path.dirname(os.path.abspath(__file__)), '../twitter/twitter_key.yml']))

        follow = twitter_service.get_following(screen_name='Sentifi_UK')
        follow_screenname = twitter_service.get_screen_names_by_ids(follow)
        fo = open('/home/nhat/data/result/sentifi_following.csv', 'w')
        for i in range(len(follow_screenname)):
            fo.write(str(follow[i]) + ',' + follow_screenname[i] + '\n')
        fo.close()

        follow = twitter_service.get_follower(screen_name='Sentifi_UK')
        follow_screenname = twitter_service.get_screen_names_by_ids(follow)
        fo = open('/home/nhat/data/result/sentifi_follower.csv', 'w')
        for i in range(len(follow_screenname)):
            fo.write(str(follow[i]) + ',' + follow_screenname[i] + '\n')
        fo.close()

    def id_to_screenname(self):
        pd_file = pd.read_csv('/home/nhat/data/20150706_1990 released publishers_multiplesns.csv')
        twitter_service = TwitterService('/'.join([os.path.dirname(os.path.abspath(__file__)), '../twitter/twitter_key.yml']))
        list_id = []
        for id in pd_file['id']:
            list_id.append(id)

        list_screenname = twitter_service.get_screen_names_by_ids(list_id)
        fo = open('/home/nhat/data/result/screen_name.csv', 'w')
        for i in range(len(list_screenname)):
            fo.write(str(list_id[i]) + ',' + list_screenname[i] + '\n')
        fo.close()

    def screenname_to_id(self):
        pd_file = pd.read_excel('/home/diepdt/crawl_data/151023_crawl_publisher_profile/151023_crawl_publisher_profile.xlsx')
        twitter_service = TwitterService('/'.join([os.path.dirname(os.path.abspath(__file__)), '../twitter/twitter_key.yml']))
        list_screenname = []
        for scr in pd_file['screen_name']:
            if not my_helper.pandas_null(scr):
                list_screenname.append(scr)

        list_id, list_screenname = twitter_service.get_ids_screen_names(list_screen_names=list_screenname)
        fo = open('/home/diepdt/crawl_data/151023_crawl_publisher_profile/151023_crawl_publisher_profile.csv', 'w')
        fo.write('sns_id' + ',' + 'screen_name' + '\n')
        for i in range(len(list_screenname)):
            fo.write(str(list_id[i]) + ',' + list_screenname[i] + '\n')
        fo.close()

    def screenname_to_ids_post_queue(self):
        twitter_service = TwitterService('/'.join([os.path.dirname(os.path.abspath(__file__)), '../twitter/twitter_key.yml']))

        screennames =  ['davidmcw', 'anthonyjevans', 'jmateosgarcia', 'jomicheii', 'antbreach', 'jerome_booth', 'katieuevans', 'davidskarbek', 'sgifforduk', 'simontilford', 'jp_koning', 'timleunig', 'darioperkins', 'andrew_lilico', 'howardarcheruk', 'simonniesr', 'felicityburch', 'hermeschiefecon', 'mcmahonecon', 'mattwhittakerrf', 'craigpholmes', 'stephleroux', 'gpontin', 'annastupnytska', 'charlesdumaslsr', 'mike_t_sanders', 'choyleva', 'michaelgdall', 'petersainsbury7', 'mikecampbell3', 'sandrabernick', 'barker4kate', 'colin_cwilliams', 'angusarmstrong8', 'haskelecon', 'rd_economist', 'iamvickypryce', 'mikebreweressex', 'adamcorlett', 'shjfrench', 'sukidil', 'fiveminuteecon', 'markcliffe', 'grantfitzner',
                        'pdosullivan', 'geraintjohnes', 'ianstewartecon', 'pdmsero', 'clausvistesen', 'jobseconomist', 'theneweconomist', 'robertjenkinsuk', '_stephendevlin', 'jbhearn', 'retepelyod', 'johnfingleton1', 'mj_oakley', 'jzuccollo', 'adammemoncps', 'markgregoryey', 'phbent', 'garrickhileman', 'kansoy', 'john_mills_jml', 'emilyskarbek', 'danrcorry', 'ianshepherdson', 'komileva', 'gabrielsterne', 'kproductivity', 'mjhsinclair', 'jonathan_todd', 'leecrawfurd', 'williamsonchris', 'alastairwinter', 'meadwaj', 'rskidelsky', 'jappleby123', 'k_niemietz', 'johnvanreenen', 'rencapman', 'bswud', 'mrrbourne', 'kingeconomist', 'jedieconomist', 'cjfdillow', 'worstall', 'williamnhutton', 't0nyyates', 'ronanlyons', 'asentance',
                        'richardwellings', 'owenbarder', 's8mb', 'diane1859', 'whelankarl', 'georgemagnus1', 'jkaonline', 'jdportes', 'profstevekeen', 'ajgoodwin1', 'grahambrownlow', 'geoffreydicks', 'philip_b_ball', 'adriancooperoe', 'csddavis', 'joblanden', 'cliochris', 'brosewell', 'worth_jack', 'britmouse', 'richarddisney', 'derconomy', 'proftevans', 'daveramsden1', 'gabbystein', 'richbarwell', 'kerndavid', 'gvlieghe', 'audgbp', 'rogerbootle', 'blondecoluk', 'moniqueebell', 'markwfranks', 'liz_mckeown', 'andrewsmithecon', 'doctormister']
        user_ids = twitter_service.get_ids_by_screen_names(screennames)

        screennames = ['eileentso', 'cgledhill', 'chris_skinner', 'gcgodfrey', 'davidbrear', 'dgwbirch', 'obussmann', 'rogerkver', 'michaeltinmouth', 'collision', 'sytaylor', 'neirajones', 'duenablomstrom', 'garyturner', 'robmoff', 'sion_smith', 'brian_armstrong', 'daveg', 'devie_mohan', 'ccalmeja', 'prestonjbyrne', 'darylandhobbes', 'lizlum', 'duediler', 'ruzbehb', 'gendal', 'russshaw1', 'sebfintech', 'dianacbiggs', 'jonas', 'chiewnz', 'aneeshvarma', 'gerardgrech', 'naszub', 'taavet', 'duanejackson', 'gooldrichard', 'christianfaes', 'geoffmiller66', 'andrasonea', 'cmogle', 'philballen', 'oikonomics', 'huynguyentrieu', 'marovdan', 'pgelis', 'jeffseedrs', 'edmaughan', 'nickhungerford', 'claudiabate', 'nehamanaktala', 'sophieguibaud', 'bradvanl', 'fehrsam', 'belimad',
                      'stuartheyworth', 'kaarmann', 'rebmelmen', 'alipaterson', 'ash_danga', 'bradbox', 'peterjoelkent', 'nekliolios', 'sebsutherland', 'tjwoolf', 'pettymark', 'jasonbates', 'marklazar1', 'jessinblue', 'gagan', 'financeconrad', 'lukelang', 'louisehbeaumont', 'yoniassia', 'stuartclarkeuk', 'swombat', 'matteorizzi', 'sevendotzero', 'aden_76', 'ahatami', 'mellinghoff', 'prateek_london', 'clairecockerton', 'frankseedrs', 'mmqlondon', 'cflynnlevy', 'timlevene', 'benaronsten', 'onelifenofear', 'willwynne', 'andrewcarrier', 'jamesvarga', 'davidimportio', 'zopagiles', 't_blom', 'prmonkeyman', 'cecj', 'gpn01', 'benmillerise', 'susannechishti', 'risedotglobal',
                      'furnisso', 'alanmeaney', 'cmsilva', 'samirdesai01', 'stevelemon2', 'nicklevine', 'juliasgroves', 'scottmurphy89', 'mattjackrob', 'abdulhbasit', 'anilstocker', 'dazwest', 'jaysshortt', 'andrewfogg', 'mmeentrepreneur', 'nigelverdon', 'markwhitcroft', 'deamarkova', 'richard_nostrum', 'cloudmargin', 'markdavidlamb', 'patersonae', 'henry_freeman', 'payah', 'emanuelandjelic', 'sir_resh', 'barryejames', 'toni_rami', 'timfouracre', 'spenceliam', 'hirokitakeuchi', 'alf05ter', '_bharat_', 'jamescrowley', 'sirteno', 'laurenceaderemi', 'nicolakoronka', 'dgathandpoint', 'jamesgreenthing', 'traverscw', 'dannystagg', 'docjamessmith', 'jarscott8', 'matthewpainter',
                      'jwtuckett', 'ritskov', 'tomrobin', 'alex_ffrees', 'atmikeb', 'jrglandbay', 'doraziexplora', 'mgazeley', 'danmorgan1', 'juliamorrongiel', 'ericvanderkleij', 'jimmychappell', 'matthew_ford1', 'olliepurdue', 'apwfa', 'mrfintech', 'peerster', 'alickvarma', 'cfxfurlong', 'virrajjatania', 'dwrbrown', 'graystern', 'zarinekharas', 'adambraggs', 'juancolonbolea', 'misskrissybb', 'samjgordon', 'natasha__martin', 'mikechoj', 'therealpashton', 'mauliksailor', 'alexapontikis', '_georgeanastasi', 'hannahtrevs', 'christoph_iwoca', 'albertcreixell', 'alexanderross', 'johngbooth', 'xtianerlandson', 'risedotglobal']
        user_ids += twitter_service.get_ids_by_screen_names(screennames)


        queue = Queue({
            'uri':['amqp://worker:onTheStage@rabbitmq-orange-01.ireland.sentifi.internal'],
            'name':'DailyBatchTwitterProfileCrawlByTwitterId_PotentialPublishersExchange' })
        for user_id in user_ids:
            try:
                queue.post(str(user_id))
            except:
                pass

        queue.close()

    def crawl_profile_by_screen_names(self):
        pd_file = pd.read_excel('/home/diepdt/crawl_data/151023_crawl_publisher_profile/151023_crawl_publisher_profile.xlsx')
        twitter_service = TwitterService('/'.join([os.path.dirname(os.path.abspath(__file__)), '../twitter/twitter_key.yml']))
        pd_file['sns_id'] = ''
        pd_file['description'] = ''
        pd_file['address'] = ''
        pd_file['url'] = ''
        pd_file['existed_publisher'] = ''
        # sns_profiles = twitter_service.get_profiles(sns_ids)
        screen_names = [str(x.strip()) for x in pd_file['screen_name'].tolist()]
        sns_profiles = twitter_service.get_profiles_by_screen_names(screen_names)
        sns_ids = [str(x['id']) for x in sns_profiles.values()]
        existed_ids = self.check_already_publisher(sns_ids)
        for idx, row in pd_file.iterrows():
            screen_name = str(row['screen_name'].strip().lower())
            dic = sns_profiles.get(screen_name)
            if dic:
                sns_id = dic.get('id_str')
                pd_file.loc[idx, 'sns_id'] = sns_id
                pd_file.loc[idx, 'description'] = dic.get('description')
                pd_file.loc[idx, 'address'] = dic.get('location')
                try:
                    pd_file.loc[idx, 'url'] = dic['entities']['url']['urls'][0].get('expanded_url')
                except KeyError:
                    pass

                if str(sns_id) in existed_ids:
                    pd_file.loc[idx, 'existed_publisher'] = 'Yes'

        pd_file.to_excel('/home/diepdt/crawl_data/151023_crawl_publisher_profile/151023_crawl_publisher_profile_.out.xls',
                         index=False, encoding='utf-8')

    def crawl_profile_by_sns_ids(self):
        pd_file = pd.read_excel('/home/diepdt/crawl_data/151019_crawl_publisher_profile/151019_700_Gruenderszene.xlsx')
        twitter_service = TwitterService('/'.join([os.path.dirname(os.path.abspath(__file__)), '../twitter/twitter_key.yml']))
        pd_file['description'] = ''
        pd_file['address'] = ''
        pd_file['existed_publisher'] = ''
        sns_ids = [str(x) for x in pd_file['sns_id'].tolist()]
        existed_ids = self.check_already_publisher(sns_ids)
        sns_profiles = twitter_service.get_profiles(sns_ids)
        print len(existed_ids)
        for idx, row in pd_file.iterrows():
            sns_id = str(row['sns_id'])
            dic = sns_profiles.get(sns_id)
            if dic:
                pd_file.loc[idx, 'description'] = dic.get('description')
                pd_file.loc[idx, 'address'] = dic.get('location')
            if str(sns_id) in existed_ids:
                pd_file.loc[idx, 'existed_publisher'] = 'Yes'

        pd_file.to_excel('/home/diepdt/crawl_data/151019_crawl_publisher_profile/151019_700_Gruenderszene_with_profile.xls',
                         index=False, encoding='utf-8')

    def check_already_publisher(self, sns_ids):
        existed_ids = set()
        with conn.psql1_connection(database='rest_in_peace') as pg_conn:
            with pg_conn.cursor() as pg_cur:
                pg_cur.execute('''SELECT sns_id FROM sns_account WHERE sns_name = 'tw' AND sns_id = ANY(%s)''',
                               (sns_ids,))
                pg_conn.commit()
                rows = pg_cur.fetchall()
                for row in rows:
                    existed_ids.add(row[0])
        return existed_ids

if __name__ == '__main__':
    run = RunService()
    # run.screenname_to_id()
    # run.crawl_profile_by_screen_names()
