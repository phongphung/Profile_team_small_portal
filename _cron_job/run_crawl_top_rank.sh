#!/usr/bin/env bash

cd ~
source .bashrc
python2.7 automation/crawl_top_rank.py
deactivate