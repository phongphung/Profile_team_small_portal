__author__ = 'sunary'


BROKER_URL = 'amqp://sentifi:onTheStage@10.1.1.101:5672//'

# Using Redis to store task state and results.
CELERY_RESULT_BACKEND = 'redis://ranking.ssh.sentifi.com:6379'

CELERY_ROUTES = {
    'sentifi.ranking.tasks.cache.cache': {
        'queue': 'Sentifi_Ranking_Cache'
    },
    'sentifi.ranking.tasks.persist.persist': {
        'queue': 'Sentifi_Ranking_Persist'
    },
    'sentifi.ranking.tasks.index.index': {
        'queue': 'Sentifi_Ranking_Index'
    }
}

CELERY_DISABLE_RATE_LIMITS = True

CELERY_TASK_RESULT_EXPIRES = 60 * 5

CELERY_INCLUDE = ['sentifi.ranking.tasks.reach',
                  'sentifi.ranking.tasks.interaction',
                  'sentifi.ranking.tasks.scoring',
                  'sentifi.ranking.tasks.main',
                  'sentifi.ranking.tasks.persist',
                  'sentifi.ranking.tasks.cache',
                  'sentifi.ranking.tasks.topic']