#!/usr/bin/env python
# -*-coding:UTF-8 -*
"""
Queue helper module
===================

This module subscribe to a Publisher stream and put the received messages
into a Redis-list waiting to be popped later by others scripts.
"""
import redis
import ConfigParser
import zmq
import time
import json


class PubSub(object):

    def __init__(self, config):
        self.config = config
        self.redis_sub = False
        self.zmq_sub = False
        self.subscriber = None
        self.publishers = {'Redis': [], 'ZMQ': []}

    def _get_channel(self, queue_name):
        if self.config.has_option(queue_name, 'channel'):
            channel = self.config.get(queue_name, 'channel')
        else:
            channel = queue_name.split('_')[1]
        return channel

    def _get_redis(self, queue_name):
        if not self.config.has_section(queue_name):
            section_name = 'Redis_Default'
        else:
            section_name = queue_name
        host = self.config.get(section_name, 'host')
        port = self.config.get(section_name, 'port')
        db = self.config.get(section_name, 'db')
        r = redis.StrictRedis(host=host, port=port, db=db)
        return r

    def setup_subscribe(self, queue_name):
        channel = self._get_channel(queue_name)
        if queue_name.startswith('Redis'):
            self.redis_sub = True
            r = self._get_redis(queue_name)
            self.subscriber = r.pubsub(ignore_subscribe_messages=True)
            self.subscriber.psubscribe(channel)
        elif queue_name.startswith('ZMQ'):
            self.zmq_sub = True
            context = zmq.Context()
            self.subscriber = context.socket(zmq.SUB)
            self.subscriber.connect(self.config.get(queue_name, 'address'))
            self.subscriber.setsockopt(zmq.SUBSCRIBE, channel)

    def setup_publish(self, queue_name):
        channel = self._get_channel(queue_name)
        if queue_name.startswith('Redis'):
            r = self._get_redis(queue_name)
            self.publishers['Redis'].append((r, channel))
        elif queue_name.startswith('ZMQ'):
            context = zmq.Context()
            p = context.socket(zmq.PUB)
            p.bind(self.config.get(queue_name, 'address'))
            self.publishers['ZMQ'].append((p, channel))

    def publish(self, message):
        m = json.loads(message)
        channel_message = m.get('channel')
        for p, channel in self.publishers['Redis']:
            if channel_message is None or channel_message == channel:
                p.publish(channel, m['message'])
        for p, channel in self.publishers['ZMQ']:
            if channel_message is None or channel_message == channel:
                p.send('{} {}'.format(channel, m['message']))

    def subscribe(self):
        if self.redis_sub:
            for msg in self.subscriber.listen():
                if msg.get('data', None) is not None:
                    yield msg['data']
        elif self.zmq_sub:
            while True:
                msg = self.subscriber.recv()
                yield msg.split(' ', 1)[1]
        else:
            raise Exception('No subscribe function defined')


class Process(object):

    def __init__(self, configfile, section):
        self.modules = ConfigParser.ConfigParser()
        self.modules.read(configfile)
        self.subscriber_name = section
        self.pubsub = None
        if self.modules.has_section(section):
            self.pubsub = PubSub(self.modules)
        else:
            raise Exception('Your process has to listen to at least one feed.')
        self.r_temp = redis.StrictRedis(
            host=self.config.get('Redis_Default', 'host'),
            port=self.config.get('Redis_Default', 'port'),
            db=self.config.get('Redis_Default', 'db'))

    def populate_set_in(self):
        # monoproc
        src = self.modules.get(self.subscriber_name, 'subscribe')
        self.pubsub.setup_subscribe(src)
        for msg in self.pubsub.subscribe():
            in_set = self.subscriber_name + 'in'
            self.r_temp.sadd(in_set, msg)
            self.r_temp.hset('queues', self.subscriber_name,
                             int(self.r_temp.scard(in_set)))

    def pop_from_set(self):
        # multiproc
        in_set = self.subscriber_name + 'in'
        self.r_temp.hset('queues', self.subscriber_name,
                         int(self.r_temp.scard(in_set)))
        return self.r_temp.spop(in_set)

    def populate_set_out(self, msg, channel=None):
        # multiproc
        msg = {'message': msg}
        if channel is not None:
            msg.update({'channel': channel})
        self.r_temp.sadd(self.subscriber_name + 'out', json.dumps(msg))

    def publish(self):
        # monoproc
        if not self.modules.has_option(self.subscriber_name, 'publish'):
            return False
        dest = self.modules.get(self.subscriber_name, 'publish')
        # We can have multiple publisher
        for name in dest.split(','):
            self.pubsub.setup_publish(name)
        while True:
            message = self.r_temp.spop(self.subscriber_name + 'out')
            if message is None:
                time.sleep(1)
                continue
            self.pubsub.publish(message)
