#!/bin/bash

set -e
set -x

REDIS_HOME='/home/raphael/gits/disque/src'

${REDIS_HOME}/disque-server ./disque.conf
