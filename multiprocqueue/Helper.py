#!/usr/bin/env python
# -*-coding:UTF-8 -*
"""
Queue helper module
===================

This module subscribe to a Publisher stream and put the received messages
into a Redis-list waiting to be popped later by others scripts.
"""
import redis
import zmq
import time
import json


class PubSub(object):

    def __init__(self):
        self.redis_sub = False
        self.zmq_sub = False
        self.subscriber = None
        self.publishers = {'Redis': [], 'ZMQ': []}

    def _get_channel(self, queue_name, queue_config):
        if queue_config.get('channel'):
            channel = queue_config.get('channel')
        else:
            channel = queue_name.split('_')[1]
        return channel

    def setup_subscribe(self, queue_name, queue_config):
        channel = self._get_channel(queue_name, queue_config)
        if queue_name.startswith('Redis'):
            self.redis_sub = True
            r = redis.StrictRedis(host=queue_config['host'],
                                  port=queue_config['port'],
                                  db=queue_config['db'])
            self.subscriber = r.pubsub(ignore_subscribe_messages=True)
            self.subscriber.psubscribe(channel)
        elif queue_name.startswith('ZMQ'):
            self.zmq_sub = True
            context = zmq.Context()
            self.subscriber = context.socket(zmq.SUB)
            self.subscriber.connect(queue_config.get('address'))
            self.subscriber.setsockopt_string(zmq.SUBSCRIBE, channel)

    def subscribe(self):
        if self.redis_sub:
            for msg in self.subscriber.listen():
                if msg.get('data', None) is not None:
                    yield msg['data']
        elif self.zmq_sub:
            while True:
                msg = self.subscriber.recv_string()
                yield msg.split(' ', 1)[1]
        else:
            raise Exception('No subscribe function defined')

    def setup_publish(self, queue_name, queue_config):
        channel = self._get_channel(queue_name, queue_config)
        if queue_name.startswith('Redis'):
            r = redis.StrictRedis(host=queue_config['host'],
                                  port=queue_config['port'],
                                  db=queue_config['db'])
            self.publishers['Redis'].append((r, channel))
        elif queue_name.startswith('ZMQ'):
            context = zmq.Context()
            p = context.socket(zmq.PUB)
            p.bind(queue_config.get('address'))
            self.publishers['ZMQ'].append((p, channel))

    def publish(self, message):
        m = json.loads(message)
        channel_message = m.get('channel')
        for p, channel in self.publishers['Redis']:
            if channel_message is None or channel_message == channel:
                p.publish(channel, m['message'])
        for p, channel in self.publishers['ZMQ']:
            if channel_message is None or channel_message == channel:
                p.send_string('{} {}'.format(channel, m['message']))


class Pipeline(object):

    def __init__(self, redis_config, module_name):
        self.host = redis_config['host']
        self.port = redis_config['port']
        self.db = redis_config['db']
        self.module_name = module_name

        self.in_set = self.module_name + 'in'
        self.out_set = self.module_name + 'out'

        self.r_temp = redis.StrictRedis(host=self.host, port=self.port, db=self.db, socket_timeout=50000)

    def sleep(self, interval):
        """Requests the pipeline to sleep for the given interval"""
        time.sleep(interval)

    def send(self, msg, channel=None):
        '''Push a messages to the temporary exit queue (multiprocess)'''
        msg = {'message': msg.decode('utf-8')}
        if channel is not None:
            msg.update({'channel': channel})
        self.r_temp.sadd(self.out_set, json.dumps(msg))

    def receive(self):
        '''Pop a messages from the temporary queue (multiprocess)'''
        # Update the size of the current waiting queue (for information purposes)
        self.r_temp.hset('queues', self.module_name, self.count_queued_messages())
        return self.r_temp.spop(self.in_set)

    def count_queued_messages(self):
        '''Return the size of the current queue'''
        return self.r_temp.scard(self.in_set)


class Process(object):

    def __init__(self, pipeline, module_name, runtime):
        self.modules = json.load(open(pipeline, 'r'))
        self.runtime = json.load(open(runtime, 'r'))
        self.module_name = module_name
        self.pubsub = PubSub()
        # Setup the intermediary redis connector that makes the queues multiprocessing-ready
        self.r_temp = redis.StrictRedis(host=self.runtime['Redis_Default']['host'],
                                        port=self.runtime['Redis_Default']['port'],
                                        db=self.runtime['Redis_Default']['db'])
        self.in_set = self.module_name + 'in'
        self.out_set = self.module_name + 'out'

    def populate_set_in(self):
        '''Push all the messages addressed to the queue in a temporary redis set (mono process)'''
        src = self.modules[self.module_name].get('source-queue')
        queue_config = self.runtime.get(src)
        if queue_config is None:
            queue_config = self.runtime['Redis_Default']
        self.pubsub.setup_subscribe(src, queue_config)
        for msg in self.pubsub.subscribe():
            self.r_temp.sadd(self.in_set, msg)
            self.r_temp.hset('queues', self.module_name, int(self.r_temp.scard(self.in_set)))

    def publish(self):
        '''Push all the messages processed by the module to the next queue (mono process)'''
        destinations = self.modules[self.module_name].get('destination-queues')
        if destinations is None:
            return False
        # We can have multiple publisher
        for dst in destinations:
            queue_config = self.runtime.get(dst)
            if queue_config is None:
                queue_config = self.runtime['Redis_Default']
            self.pubsub.setup_publish(dst, queue_config)
        while True:
            message = self.r_temp.spop(self.out_set)
            if message is None:
                time.sleep(1)
                continue
            self.pubsub.publish(message.decode('utf-8'))
