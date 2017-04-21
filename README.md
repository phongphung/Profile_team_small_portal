# Setup

	$ /usr/local/bin/virtualenv worker
	$ ./worker/bin/pip install -r requirements.txt

# Post to queue to crawl 200 tweets

	$ export PYTHONPATH=`pwd`
	$ ./worker/bin/python queue/queue_crawl_200_tweets.py
	
Note: Ensure that the queue name, number messages to post is correct. Currently, it is hard code in file `queue_crawl_200_tweets.py`