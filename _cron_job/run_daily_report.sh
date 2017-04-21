#!/usr/bin/env bash

cd ~
source .bashrc
python2.7 automation/daily_messages_report.py
python2.7 automation/daily_qualified_report.py
deactivate