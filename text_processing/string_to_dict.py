# -*- coding: utf-8 -*-
__author__ = 'sunary'


import ast


class StringToDict():

    def __init__(self):
        pass

    def to_dict(self):
        string = '[{"tel":"21163534","fax":"21511778","email":"dheron@amcapital.hk","address":{"fullAddress":"19/F, 8 Lyndhurst Terrace, Central, Hong Kong","fullAddressChin":"香港中環擺花街8號19樓","centralEntity":null}}]'
        string = string.replace('null', '""')
        print ast.literal_eval(string)


if __name__ == '__main__':
    to_dict = StringToDict()
    to_dict.to_dict()