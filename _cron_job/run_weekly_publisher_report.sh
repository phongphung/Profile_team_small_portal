#!/usr/bin/env bash

cd ~
source .bashrc
python2.7 automation/weekly_publisher_report.py
deactivate