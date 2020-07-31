#!/usr/bin/env python
from concurrent.futures import ThreadPoolExecutor

from logzero import logger

import magellan_crawler_mirror_site
from solution_fast_get_distribute import AMQPConnection

#Create Core
CoreCrawler = magellan_crawler_mirror_site.MirrorSite("",1)
#Init Connection RabbitMQ
connection = AMQPConnection.connectAMQP()
channel = connection.channel()

print(' [*] Waiting for logs. To exit press CTRL+C')

class WorkerCrawl:
    def __init__(self,numberWorker):
        self.numberWorker = numberWorker
        # Create Thread Pool
        self.pool = ThreadPoolExecutor(max_workers=numberWorker)
        channel.exchange_declare(exchange='link', exchange_type='topic')
        result = channel.queue_declare(queue='worker', exclusive=False)
        queue_name = result.method.queue
        channel.queue_bind(exchange='link', queue=queue_name)
        channel.basic_qos(prefetch_count=1)
        channel.basic_consume(queue=queue_name, on_message_callback=self.callback, auto_ack=True)
        channel.start_consuming()

    def process(self,content):
        logger.info("{}".format(content.decode()))
        for content, url in CoreCrawler.recursive_ver_2(content.decode()):
            CoreCrawler.save(content, url)
    def callback(self,ch, method, properties, content):
        logger.info("{}".format(content.decode()))
        #MULTI THREADING
        job = self.pool.submit(self.process(content))
