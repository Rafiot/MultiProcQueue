#!/usr/bin/env python
# -*- coding: utf-8 -*-

import time

from pydisque.client import Client

client = Client(["127.0.0.1:7711"])
client.connect()

nb = 0

while True:
    messagedata = time.time()
    client.add_job("Global", messagedata, async=True)
    nb += 1
    #time.sleep(.001)
    if nb % 1000 == 0:
        print(nb)

    if nb % 10000 == 0:
        print('Done.')
        break
