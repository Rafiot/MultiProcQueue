#!/usr/bin/env python
# -*-coding:UTF-8 -*


import json
import subprocess
import time
import argparse


def check_pid(pid):
    if pid is None:
        # Already seen as finished.
        return None
    else:
        if pid.poll() is not None:
            return False
    return True


def run(pipeline, runtime):
    config = json.load(open(pipeline, 'r'))

    pids = {}
    for module in config.keys():
        pin = subprocess.Popen(['QueueIn.py', '-p', pipeline, '-m', module, '-r', runtime])
        pout = subprocess.Popen(['QueueOut.py', '-p', pipeline, '-m', module, '-r', runtime])
        pids[module] = (pin, pout)
    is_running = True
    try:
        while is_running:
            time.sleep(5)
            is_running = False
            for module, p in pids.iteritems():
                pin, pout = p
                if pin is None:
                    # already dead
                    pass
                elif not check_pid(pin):
                    print(module, 'input queue finished.')
                    pin = None
                else:
                    is_running = True
                if pout is None:
                    # already dead
                    pass
                elif not check_pid(pout):
                    print(module, 'output queue finished.')
                    pout = None
                else:
                    is_running = True
                pids[module] = (pin, pout)
    except KeyboardInterrupt:
        for module, p in pids.items():
            pin, pout = p
            if pin is not None:
                pin.kill()
            if pout is not None:
                pout.kill()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Launch all the multipocessing helpers desribed in the config file.')
    parser.add_argument("-p", "--pipeline", type=str, required=True, help="Path to the pipeline configuration file.")
    parser.add_argument("-r", "--runtime", type=str, required=True, help="Path to the runtime configuration file.")
    args = parser.parse_args()

    run(args.pipeline, args.runtime)
