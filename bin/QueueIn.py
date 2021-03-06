#!/usr/bin/env python
# -*-coding:UTF-8 -*

import argparse
import signal
import sys

from pubsublogger import publisher
from multiprocqueue import Process


def signal_term_handler(signal, frame):
    publisher.info('got SIGTERM')
    sys.exit(0)


def run(pipeline, module, runtime):
    p = Process(pipeline, module, runtime)
    p.populate_set_in()


if __name__ == '__main__':
    publisher.port = 6381
    publisher.channel = 'Queuing'
    signal.signal(signal.SIGINT, signal_term_handler)

    parser = argparse.ArgumentParser(description='Input queue for a module.')
    parser.add_argument("-p", "--pipeline", type=str, required=True, help="Path to the pipeline configuration file.")
    parser.add_argument("-m", "--module", type=str, required=True, help="Module to use.")
    parser.add_argument("-r", "--runtime", type=str, required=True, help="Path to the runtime configuration file.")
    args = parser.parse_args()

    publisher.info('QueueIn: Starting ' + args.module)

    run(args.pipeline, args.module, args.runtime)
