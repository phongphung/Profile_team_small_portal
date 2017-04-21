#!/usr/bin/env bash

cd ~
source .bashrc
python2.7 automation/daily_read_redis.py
deactivate