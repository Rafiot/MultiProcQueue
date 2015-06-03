#!/bin/bash

set -e
set -x

REDIS_HOME=''

${REDIS_HOME}/redis-server ./redis.conf
