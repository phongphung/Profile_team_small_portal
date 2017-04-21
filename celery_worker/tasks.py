__author__ = 'sunary'


from celery import chain, group, chord
from main_app import app


class AddTasks(app.Task):

    def __init__(self):
        pass

    def run(self, x, y):
        return x + y

class MultiTasks(app.Task):

    def __init__(self):
        pass

    def run(self, x, y):
        return x * y

class SumTasks(app.Task):

    def __init__(self):
        pass

    def run(self, x):
        return sum(x)

if __name__ == '__main__':
    '''
    Usages:
        cd [os.path.abspath(__file__)]
        celery worker -A tasks -l info
        #celery multi start 8 -A tasks -l debug -Q Sentifi_Ranking_Cache,Sentifi_Ranking_Persist,celery --logfile=data/%N.log
        python tasks.py
    '''

    add_task = AddTasks()
    multi_task = MultiTasks()
    sum_task = SumTasks()
    res = add_task.delay(1, 2)
    print res.get()

    c = chain(add_task.s(1, 2), multi_task.s(4), multi_task.s(5))
    res = c()
    print res.get()

    g = group(add_task.s(2, 2), add_task.s(4, 4))
    res = g()
    print res.get()

    print chord(add_task.s(i, i) for i in xrange(100))(sum_task.s()).get()