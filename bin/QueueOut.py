#!/usr/bin/env python
# -*-coding:UTF-8 -*

from pubsublogger import publisher
from multiprocqueue import Process
import argparse


def run(pipeline, module, runtime):
    p = Process(pipeline, module, runtime)
    if not p.publish():
        print(module, 'has no publisher.')

if __name__ == '__main__':
    publisher.port = 6380
    publisher.channel = 'Queuing'

    parser = argparse.ArgumentParser(description='Output queue for a module.')
    parser.add_argument("-p", "--pipeline", type=str, required=True, help="Path to the pipeline configuration file.")
    parser.add_argument("-m", "--module", type=str, required=True, help="Module to use.")
    parser.add_argument("-r", "--runtime", type=str, required=True, help="Path to the runtime configuration file.")
    args = parser.parse_args()

    run(args.pipeline, args.module, args.runtime)
