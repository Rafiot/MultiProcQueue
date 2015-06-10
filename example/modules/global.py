#!/usr/bin/env python
import argparse
import json

from pubsublogger import publisher
from multiprocqueue import Pipeline


if __name__ == '__main__':
    publisher.port = 6381
    publisher.channel = 'Script'

    parser = argparse.ArgumentParser(description='General Queue.')
    parser.add_argument("-r", "--runtime", type=str, required=True, help="Path to the runtime configuration file.")
    parser.add_argument("-i", "--id", type=str, required=True, help="Module ID.")
    args = parser.parse_args()

    module_name, module_id = args.id.split('_')

    runtime = json.load(open(args.runtime, 'r'))
    pipeline = Pipeline(runtime['Redis_Default'], module_name)

    # LOGGING #
    publisher.info(module_name + ": started to receive & publish.")

    nb = 0

    while True:
        message = pipeline.receive()
        if message is not None:
            publisher.debug(module_name + ': Got a message')
            pipeline.send(message)
            nb += 1
            if nb % 100 == 0:
                publisher.info('{} ({}): {} messages processed, {} to go.'.format(
                    module_name, module_id, nb, pipeline.count_queued_messages()))
        else:
            publisher.debug(module_name + ": Empty Queues: Waiting...")
            pipeline.sleep(1)
