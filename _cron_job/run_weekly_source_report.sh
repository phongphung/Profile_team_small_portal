#!/usr/bin/env bash

cd ~
source .bashrc
python2.7 automation/weekly_source_report.py
deactivate