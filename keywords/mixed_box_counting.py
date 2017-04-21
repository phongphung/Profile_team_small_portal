__author__ = 'sunary'


import requests
import pandas as pd


class MixedBoxCounting():

    def __init__(self):
        pass

    def process(self, file_source, file_dest):
        channel = ['twitter', 'news', 'blog']

        pd_file = pd.read_csv(file_source)

        writer = 'item_id,publisher_id,publisher_name,channel,nums,priority\n'
        for item_id in pd_file['id']:
            msg_parser = self.mixed_box_messages(item_id)
            for i in range(len(msg_parser)):
                for key, value in msg_parser[i].iteritems():
                    if '___' not in key:
                        writer += '%s,%s,"%s",%s,%s,%s\n' % (item_id, key, msg_parser[i].get(key + '___fullname'), channel[i], value, msg_parser[i].get(key + '___priority'))

        writer = writer.encode(encoding='utf-8')
        with open(file_dest, 'w') as fo:
            fo.write(writer)

    def mixed_box_messages_bak(self, item_id):
        channel = ['{"type":"MESSAGES","boxName":"messages_t","channel":"twitter","top":"60","lang":"all","country":"all"}',
                   '{"type":"MESSAGES","boxName":"messages_b","channel":"blog","top":"60","lang":"all","country":"all"},'
                   '{"type":"MESSAGES","boxName":"messages_n","channel":"news","top":"60","lang":"all","country":"all"}']
        channel_attr = [['messages_t', 'twitter'], ['messages_n', 'news']]

        msg_parser = [{}, {}]
        for i in range(len(channel)):
            res = requests.get('http://restint.sentifi.com/message/mixed?callback=&child=[%s]&language=en&top=60&period=lastweek&lang=all&country=all&boxName=MIXED&isNew=true&itemkey=%s' % (channel[i], item_id))
            for msg in res.json()['data'][channel_attr[i][0]]['data'][channel_attr[i][1]]:
                try:
                    if msg_parser[i].get(msg['publisherId']):
                        msg_parser[i][msg['publisherId']] += 1
                    else:
                        msg_parser[i][msg['publisherId']] = 1
                        msg_parser[i][msg['publisherId'] + '___priority'] = msg['priority']
                        msg_parser[i][msg['publisherId'] + '___fullname'] = msg['fullname']
                except:
                    pass
        return msg_parser

    @staticmethod
    def mixed_box_messages(item_id):
        msg_parser = [{}, {}, {}]
        channel_attr = [['messages_t', 'twitter'], ['messages_n', 'news'], ['messages_b', 'blog']]
        res = requests.get('http://rii.sentifi.com/message/mixed?child=[{"type":"BUZZ","boxName":"buzz","period":"lastweek"},{"type":"CHANNELRANKING","boxName":"channelranking","period":"lastweek"},{"type":"MESSAGES","boxName":"messages_t","channel":"twitter","top":"60","lang":null,"country":null},{"type":"MESSAGES","boxName":"messages_b","channel":"blog","top":"60","lang":null,"country":null},{"type":"MESSAGES","boxName":"messages_n","channel":"news","top":"60","lang":null,"country":null}]&language=en&top=60&period=lastweek&isNew=true&itemkey=%s' % item_id)
        for i in range(len(channel_attr)):
            for msg in res.json()['data'][channel_attr[i][0]]['data'][channel_attr[i][1]]:
                try:
                    publisher_id = '%s-%s' % (msg['sns']['snsName'], msg['sns']['snsId'])
                    if msg_parser[i].get(publisher_id):
                        msg_parser[i][publisher_id] += 1
                    else:
                        msg_parser[i][publisher_id] = 1
                        msg_parser[i][publisher_id + '___priority'] = 1 if 'priority' in msg['sns']['snsTag'] else 0
                        msg_parser[i][publisher_id + '___fullname'] = msg['sns'].get('fullname')
                except:
                    pass
        return msg_parser


if __name__ == '__main__':
    mixed_box_counting = MixedBoxCounting()
    import pprint
    pprint.pprint(mixed_box_counting.mixed_box_messages(226))
    # mixed_box_counting.process('/home/nhat/data/_id.csv', '/home/nhat/data/mixed_box.csv')




