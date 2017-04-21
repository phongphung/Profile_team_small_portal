from utils.my_mongo import Mongodb

__author__ = 'diepdt'


if __name__ == '__main__':
    collection = Mongodb(db='publisher', col='gruenderszene_investor')
    collection.export_excel(['name', 'title', 'twitter', 'description'], '/home/diepdt/export_data/151015_gruenderszene/151016_gruenderszene_investor.xls')