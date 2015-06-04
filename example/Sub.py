#!/usr/bin/env python
import time
import argparse
import json

from pubsublogger import publisher
from multiprocqueue import pop_from_set, populate_set_out


if __name__ == '__main__':
    publisher.port = 6381
    publisher.channel = 'Script'

    parser = argparse.ArgumentParser(description='General Queue.')
    parser.add_argument("-r", "--runtime", type=str, required=True, help="Path to the runtime configuration file.")
    args = parser.parse_args()

    runtime = json.load(open(args.runtime, 'r'))

    module_name = 'Sub'

    # LOGGING #
    publisher.info("Feed Script started to receive & publish.")

    while True:

        message = pop_from_set(runtime, module_name)
        if message is not None:
            print(module_name, 'Got a message')
            populate_set_out(runtime, module_name, message)
        else:
            print(module_name, "Empty Queues: Waiting...")
            time.sleep(1)
            continue
