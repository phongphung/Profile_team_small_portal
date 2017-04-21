__author__ = 'sunary'


from pyspark import SparkContext


class CountWords():

    def __init__(self):
        '''
        Usages:
            cd [spark root]
            ./bin/pyspark count_words.py
        '''

    def process(self):
        log_file = '/home/nhat/spark-1.1.0/README.md'
        sc = SparkContext('local', 'Count Words')
        log_data = sc.textFile(log_file).cache()

        print log_data.count()
        print log_data.first()

        num_a = log_data.filter(lambda s: 'a' in s).count()
        num_b = log_data.filter(lambda s: 'b' in s).count()

        print '%s %s' % (num_a, num_b)
        sc.stop()

if __name__ == '__main__':
    count_words = CountWords()
    count_words.process()
