__author__ = 'sunary'


from celery import Celery


app = Celery('tasks',
             backend='redis://localhost:6379/1',
             broker='amqp://guest:guest@host:port',
             include=['tasks'])

# app.config_from_object('celery_config')

if __name__ == '__main__':
    app.start()
