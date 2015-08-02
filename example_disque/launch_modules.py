#!/usr/bin/env python
# -*- coding: utf-8 -*-

import json
import argparse
import subprocess
import time
import shlex


def check_pid(pid):
    if pid is None:
        # Already seen as finished.
        return None
    else:
        if pid.poll() is not None:
            return False
    return True


def run(startup, runtime, pipeline):
    config = json.load(open(startup, 'r'))
    pids = {}
    for module in config.keys():
        nb_processes = config[module].get('processes')
        if nb_processes is None:
            nb_processes = 1
        pids[module] = []
        for i in range(nb_processes):
            cmd = "python -m {} -r {} -i {}_{}, -p {}".format(config[module]['module'], runtime, module, i, pipeline)
            args = shlex.split(cmd)
            pid = subprocess.Popen(args)
            pids[module].append(pid)
    is_running = True
    try:
        while is_running:
            time.sleep(5)
            is_running = False
            for module, ps in pids.items():
                cur_pids = []
                for p in ps:
                    if not check_pid(p):
                        # process finished
                        continue
                    is_running = True
                    cur_pids.append(p)
                pids[module] = cur_pids
    except KeyboardInterrupt:
        for module, ps in pids.items():
            for p in ps:
                p.kill()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Manage the amount of proceses started for each module.')
    parser.add_argument("-s", "--startup", type=str, required=True, help="Path to the startup configuration file.")
    parser.add_argument("-r", "--runtime", type=str, required=True, help="Path to the runtime configuration file.")
    parser.add_argument("-p", "--pipeline", type=str, required=True, help="Path to the pipeline configuration file.")
    args = parser.parse_args()
    run(args.startup, args.runtime, args.pipeline)
