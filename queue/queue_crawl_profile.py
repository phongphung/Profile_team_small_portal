__author__ = 'sunary'


from utils import my_connection, my_helper
import time
from queue import Queue, ConsumerQueue
from utils.my_mongo import Mongodb
from twitter.twitter_services import TwitterService
import os


class QueueCrawProfile():
    def __init__(self):
        self.queue = Queue(dict(
            Uri='worker:onTheStage@rabbitmq-orange-01.ireland.sentifi.internal',
            Name='TwitterProfileCrawlByTwitterIdProcessor_PotentialPublishers'
        ), queue_name='TwitterProfileCrawlByTwitterIdProcessor_PotentialPublishers')
        # TwitterProfileCrawlByTwitterIdProcessor_PotentialPublishers

        self.con_da0 = my_connection.da0_connection()
        self.logger = my_helper.init_logger(self.__class__.__name__)
        
    def get_id_null_payload(self):
        cur_da0 = self.con_da0.cursor()

        cur_da0.execute('''
            SELECT user_id
            FROM twitter.tw_user
            WHERE payload IS NULL
        ''')
        self.res = cur_da0.fetchall()
        self.logger.info('Done: get data')

    def save_to_tmp_data_from_mongo(self):
        #empty db
        cur_da0 = self.con_da0.cursor()

        cur_da0.execute('''
            DELETE FROM twitter._user_id_temp
        ''')
        self.con_da0.commit()

        #insert into tmp db
        mongo_save = Mongodb(db='potential_id_twitter_need_200t',
                             col='user')
        res_mongo = mongo_save.find({}, ['id'])

        list_id = []
        for doc in res_mongo:
            list_id.append({'id': int(doc['id'])})

        cur_da0.executemany('''
            INSERT INTO twitter._user_id_temp(user_id)
            VALUES(%(id)s)
        ''', list_id)
        self.con_da0.commit()

        self.logger.info('Done: save to mongo')

    def get_temp_not_exist_in_profile(self):
        cur_da0 = self.con_da0.cursor()

        cur_da0.execute('''
            SELECT user_id
            FROM twitter._user_id_temp
            WHERE NOT EXISTS(
                SELECT user_id
                FROM twitter.tw_user
                WHERE tw_user.user_id = _user_id_temp.user_id)
        ''')
        self.res = cur_da0.fetchall()

        self.logger.info('Done: get temp id not exist in tw_user')

    def get_source_not_exist_in_profile(self):
        cur_da0 = self.con_da0.cursor()

        cur_da0.execute('''
            SELECT user_id
            FROM twitter.user_source us
            WHERE NOT EXISTS(
                SELECT user_id
                FROM twitter.tw_user
                WHERE tw_user.user_id = us.user_id)
        ''')
        self.res = cur_da0.fetchall()

        self.logger.info('Done: get source not exist in tw_user')

    def post_to_queue(self):
        for id, in self.res:
            try:
                self.queue.post(str(id))
            except Exception as e:
                print e

        self.logger.info('Done: push to queue')

    def post_list_to_queue(self):
        list_ids = ('301040848','89690502','386137145','41804620','804648973','401134781','1636272852','19684233','90027318','29700654','2752810242','2846878973','29173469','182916123','8793242','24702514','2179523612','125591536','164641631','13065032','2546244168','16147236','1976794279','16852924','27094142','380372947','95450756','94077854','70999056','1959510499','50633472','74683519','23014091','46197783','143198843','28555215','217822311','260602910','398651112','266556190','938636977','198830453','1908451573','39222117','19919195','556342178','2735591','28162128','64692506','21328656','246664765','2929386021','17488526','63543482','47911050','24579349','75998926','65457124','201802500','17085417','16528229','142797755','193783826','22053873','12795662','2998793679','115677771','1966634174','75378357','38627900','211990358','800685691','31997610','72466396','18177438','22833902','153067283','89475266','289175346','14782279','10251252','17000169','1517387887','2841550671','46423945','403290105','2161939834','219371185','88950590','15317755','119450291','14727836','3198095309','94334563','92770403','529538598','51036151','291671165','351572532','243655193','25508271','19902748','21277096','64248465','566354672','2879754306','356239341','2870671049','270346277','130868869','179317489','2759094747','208261495','21669440','189166086','30426094','19728281','1190920208','202981968','2974699300','2319260622','3110962731','285523429','16147236','69635282','58266891','20640994','36484338','19410823','14637094','2825347248','167296919','20604380','66519572','46231844','50400399','178175050','426738427','491791018','260702408','253540332','253713313','238152252','21741852','2983575644','14071639','22738472','253152603','75775947','15827411','18373463','84383945','1931247445','2836483682','43421016','598242424','223110908','23754854','14369310','32430434','37151522','154948410','27846330','40224563','6281882','28529704','25237156','260162926','21792153','34494454','601228138','99958640','72896539','2284380960','46189227','1465518500','33325821','19919195','236575902','16892402','37722802','3081597406','79196203','1256147300','396439107','2676339529','2359835906','18079983','44472756','2422187958','185118883','171012078','1327885549','2199637951','814632835','2175228199','189096536','315203366','570620949','199663273','216265839','284991984','728499038','20002993','153030517','351572590','1100559055','491278924','352873712','31919952','16825811','15271072','21282350','550003586','233506733','465412679','229422651','228169779','509916575','317734587','379813961','22166412','205249455','34227863','299350772','21111293','20845984','313570316','20039669','263221036','208184197','19071027','104812724','88973563','181567341','35826666','3888731','15893310','42781027','28036329','96776708','157924945','32257425','20189555','85056538','99107260','69149700','288256736','20631172','63923066','268259624','1556298630','1321743451','21250565','1017571','2362057826','77131322','21187339','53056381','2753815446','1563447102','1406613187','10212392','18946181','104276559','1182050797','1024867136','466850600','625812094','475035959','558950810','488364322','132912886','176733139','17132298','550721020','2296549723','2576986674','20204610','247424355','20137659','260230248','2392856240','2202466878','2281321495','279220781','183358561','1222595886','288858112','223383374','2239745719','628561503','557276584','493444715','402002718','61744008')
        for id in list_ids:
            try:
                print id
                self.queue.post(str(id))
                time.sleep(1)
            except Exception as e:
                print e

        self.logger.info('Done: push to queue')

    def move_id_to_another_queue(self):
        queue_remove = ConsumerQueue(dict(
            Uri='worker:onTheStage@rabbitmq-orange-01.ireland.sentifi.internal',
            Name='TwitterProfileCrawlByTwitterIdProcessor_PotentialPublishers'
        ), queue_name='TwitterProfileCrawlByTwitterIdProcessor_PotentialPublishers')

        def my_callback(body, message):
            try:
                self.queue.post(body)
            except:
                pass
            message.ack()

        queue_remove.on_message = my_callback
        queue_remove.run()

    def close_connection(self):
        self.con_da0.close()


if __name__ == '__main__':
    queue_crawl_profile = QueueCrawProfile()
    # queue_crawl_profile.save_to_tmp_data_from_mongo()
    queue_crawl_profile.get_source_not_exist_in_profile()
    queue_crawl_profile.post_to_queue()
    queue_crawl_profile.close_connection()
    # queue_crawl_profile.move_id_to_another_queue()
