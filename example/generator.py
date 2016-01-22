#!/usr/bin/env python
# -*- coding: utf-8 -*-

import zmq
import sys
import time

port = "5558"
if len(sys.argv) > 1:
    port = sys.argv[1]
    int(port)

context = zmq.Context()
socket = context.socket(zmq.XPUB)
socket.bind("tcp://127.0.0.1:%s" % port)

nb = 0

while True:
    topic = 100
    messagedata = time.time()
    socket.send_string("{} {}".format(topic, messagedata))
    nb += 1
    time.sleep(.001)
    if nb % 1000 == 0:
        print(nb)
    elif nb % 100000 == 0:
        print('Done.')
        break
