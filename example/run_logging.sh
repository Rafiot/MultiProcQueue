#!/bin/bash

set -e
set -x

log_subscriber -p 6381 -c Queuing -l ./logs/
