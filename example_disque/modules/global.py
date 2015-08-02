#!/usr/bin/env python
import argparse
import json
import time
from pubsublogger import publisher
from pydisque.client import Client


def receive(d, source_queues):
    # only accepts string
    return d.get_job([source_queues])


def send(d, destination_queues, message):
    for q in destination_queues:
        d.add_job(q, message)


if __name__ == '__main__':
    publisher.port = 6381
    publisher.channel = 'Script'

    parser = argparse.ArgumentParser(description='General Queue.')
    parser.add_argument("-r", "--runtime", type=str, required=True, help="Path to the runtime configuration file.")
    parser.add_argument("-p", "--pipeline", type=str, required=True, help="Path to the pipeline configuration file.")
    parser.add_argument("-i", "--id", type=str, required=True, help="Module ID.")
    args = parser.parse_args()

    module_name, module_id = args.id.split('_')

    runtime = json.load(open(args.runtime, 'r'))
    pipeline = json.load(open(args.pipeline, 'r'))

    disque_client = Client(runtime["Disque_Default"])
    disque_client.connect()

    # LOGGING #
    publisher.info(module_name + ": started to receive & publish.")

    nb = 0

    while True:
        messages = receive(disque_client, pipeline[module_name]["source-queue"])
        if len(messages) > 0:
            publisher.debug(module_name + ': Got a message')
            for qname, job_id, payload in messages:
                send(disque_client, pipeline[module_name]["destination-queues"], payload)
                disque_client.ack_job(job_id)
                nb += 1
                if nb % 100 == 0:
                    publisher.info('{} ({}): {} messages processed, {} to go.'.format(
                        module_name, module_id, nb, disque_client.qlen(qname)))
        else:
            publisher.debug(module_name + ": Empty Queues: Waiting...")
            time.sleep(1)
