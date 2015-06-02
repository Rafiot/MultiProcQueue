#!/usr/bin/env python
# -*-coding:UTF-8 -*


from pubsublogger import publisher
from Helper import Process
import argparse


def run(config, section):
    p = Process(config, section)
    p.populate_set_in()


if __name__ == '__main__':
    publisher.port = 6380
    publisher.channel = 'Queuing'

    parser = argparse.ArgumentParser(description='Input queue for a module.')
    parser.add_argument("-c", "--config", type=str, required=True, help="Path to the config file.")
    parser.add_argument("-s", "--section", type=str, required=True, help="Section in the config file.")
    args = parser.parse_args()

    run(args.config, args.section)
