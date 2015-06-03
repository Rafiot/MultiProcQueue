#!/usr/bin/env python
# -*- coding: utf-8 -*-

import zmq
import random
import sys
import time

port = "5558"
if len(sys.argv) > 1:
    port = sys.argv[1]
    int(port)

context = zmq.Context()
socket = context.socket(zmq.XPUB)
socket.bind("tcp://127.0.0.1:%s" % port)


while True:
    topic = 100
    messagedata = random.randrange(1, 215) - 80
    print "%d %d" % (topic, messagedata)
    socket.send("%d %d" % (topic, messagedata))
    time.sleep(1)
