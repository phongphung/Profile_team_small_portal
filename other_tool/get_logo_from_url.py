__author__ = 'sunary'


import pandas as pd
import subprocess
from utils import my_helper, my_text
import os
from os.path import isfile, join
from PIL import Image
import time


class GetLogo():

    def __init__(self):
        self.logger = my_helper.init_logger(self.__class__.__name__)
        self.file_dest_path = None

    def process(self, file_source, file_dest_path):
        pd_file = pd.read_csv(file_source)
        self.file_dest_path = file_dest_path

        for i in range(len(pd_file['url'])):
            self.get_logo(pd_file['sns_id'][i], str(pd_file['url'][i]))

    def get_logo(self, id, url):
        img_size = 360
        url = my_text.root_url(url)

        try:
            subprocess.Popen('curl "https://logo.clearbit.com/%s?size=%s" > news_%s.png' %(url, img_size, id),
                            cwd=self.file_dest_path,
                            shell=True)
            time.sleep(1)
            self.logger.info('url: %s -- Completed' % (url))
        except Exception as e:
            self.logger.info('url: %s -- Error: %s' % (url, e))

    def delete_invalied_avatar(self, file_dest_path):
        avatar_files = [join(file_dest_path, f) for f in os.listdir(file_dest_path) if isfile(join(file_dest_path, f))]
        print len(avatar_files)

        count_invalid = 0
        for f in avatar_files:
            if os.stat(f).st_size < 1024:
                os.remove(f)
                count_invalid += 1
            else:
                try:
                    image = Image.open(f)
                    image.verify()
                except:
                    os.remove(f)
                    count_invalid += 1

        print count_invalid

    def rename_avatar(self, file_dest_path):
        avatar_files = [f for f in os.listdir(file_dest_path) if isfile(join(file_dest_path, f))]
        print 'news_52d4c05ee4b08a17cb0cc70c.png' in avatar_files
        for f in avatar_files:
            os.rename(join(file_dest_path, f), join(file_dest_path, f.replace('id_', 'news_')))

    def update_into_csv(self, file_source, file_dest, file_dest_path):
        sentifi_dir = 'https://img4.sentifi.com/avatar/'
        avatar_files = [f for f in os.listdir(file_dest_path) if isfile(join(file_dest_path, f))]
        avatar_ids = set([x.replace('news_', '').replace('.png', '') for x in avatar_files])

        generate_dataframe = {}
        generate_dataframe_fields = my_helper.get_dataframe_columns(file_source)
        for field in generate_dataframe_fields:
            generate_dataframe[field] = []

        pd_file = pd.read_csv(file_source)

        for i in range(len(pd_file[generate_dataframe_fields[0]])):
            for field in generate_dataframe_fields[:-1]:
                generate_dataframe[field].append(pd_file[field][i])

            if pd_file[generate_dataframe_fields[1]][i] in avatar_ids:
                generate_dataframe[generate_dataframe_fields[-1]].append(sentifi_dir + 'news_' + pd_file[generate_dataframe_fields[1]][i] + '.png')
            else:
                generate_dataframe[generate_dataframe_fields[-1]].append(pd_file[generate_dataframe_fields[-1]][i])

        pd_file = pd.DataFrame(data=generate_dataframe, columns=generate_dataframe_fields)
        pd_file.to_csv(file_dest, index=False)


if __name__ == '__main__':
    get_logo = GetLogo()
    # get_logo.get_logo('stripe', 'stripe.com')
    # get_logo.process('/home/nhat/data/news_avatar_url.csv', '/home/nhat/data/avatar/')
    # get_logo.delete_invalied_avatar('/home/nhat/data/avatar/')
    # get_logo.rename_avatar('/home/nhat/data/avatar/')
    get_logo.update_into_csv('/home/nhat/data/news_avatar_url.csv', '/home/nhat/data/update_avatar.csv', '/home/nhat/data/avatar/')