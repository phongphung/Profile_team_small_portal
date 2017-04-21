#!/usr/bin/env python
# -*- coding: utf-8 -*-
__author__ = 'markus'


import re
import Queue
import sys
import os
from other_tool.isocode import Graph


sys.path.append(os.path.dirname(__file__))
sys.modules['Graph'] = Graph

class PreProcess:
    _re_split = re.compile('[,/\-@\t\|&]', re.UNICODE)
    _re_split_mult = re.compile('[,/\-@\t& ]+', re.UNICODE)
    _re_brackets = re.compile('\((.*)\)', re.UNICODE)
    _re_coord = re.compile('([-+]?[0-9]*\.?[0-9]+) *, *([-+]?[0-9]*\.?[0-9]+)', re.UNICODE)
    _re_normalize_ws = re.compile('\s+', re.UNICODE)
    _re_title_words = re.compile('[\s-]+', re.UNICODE)
    _re_clean_string = re.compile('\s+', re.UNICODE)

    def __init__(self, str):
        # setup member variables
        self.str = str.title()
        self.queue = Queue.PriorityQueue()
        self.already_tried = dict()
        # run the proprocessor
        self.preprocess(self.str)

    def remove_brackets(self, s):
        """ remove brackets, turn them into commas, i.e. turn 'Nigeria (Kaduna)' into 'Nigeria, Kaduna' """
        while True:
            m = self._re_brackets.search(s)
            if m is None:
                break
            s = s.replace(m.group(), ',%s,' % m.groups()[0])
        s = s.strip(', ')
        return s

    def match_coord(self, s):
        """
        matches geo coordinate (long, lat) and returns long lat float.
        @param s: String to do match on
        @return: (True/False, long, lat)
        """
        m = self._re_coord.search(s)
        if m is None:
            return False, None, None
        else:
            return True, float(m.groups()[0]), float(m.groups()[1])

    def title_words(self, s):
        """
        Capitalize all words in string s with more than three letters, i.e. turn
        "hello you world" into "Hello you World".
        @param s: String to capitalize
        @return: The capitalized string
        """
        return s.title()
        # return ' '.join(map(lambda x: (len(x) > 3 and x.lower().title()) or x, self._re_title_words.split(s)))

    def group(self, l):
        """ expand neighbor-grouping,
            i.e. [1,2,3,4] -> ([[1,2],3,4], [[1,[2,3],4], [1,2,[3,4]]) """
        out = []
        for i in xrange(len(l) - 1):
            if i - 1 < 0:
                left = []
            else:
                left = [[x] for x in l[0:i]]
            mid = [l[i:i + 2]]
            right = [[x] for x in l[i + 2:]]
            left.extend(mid)
            left.extend(right)
            out.append(left)
        return out

    def vary(self, l):
        """
        vary the list of strings using the group function, i.e.
        ['a','b','c'] -> [['a b', 'c'], ['a', 'b c']]
        @param l: Input list
        @return: List, the items of which are the variations
        """
        return [[' '.join(item) for item in ll] for ll in self.group(l)]

    def __push_queue(self, item, prio=0, push_titled_version=False):
        """
        append an item to the queue and add it to the already_tried dict
        @param item: Item to put onto the queue
        @param prio: Priority to give this item (lower is 'better')
        """
        item_tuple = tuple(item)
        if not item_tuple in self.already_tried:
            self.queue.put((prio, item))
            # sys.stderr.write("  push %s, prio=%d\n" %(item, prio))
            self.already_tried[item_tuple] = 1

        if push_titled_version:
            # now we also push the titled version of the string-list:
            self.__push_queue(map(self.title_words, item), prio, push_titled_version=False)

    def clean(self, str):
        """
        cleans up a string, i.e. removes weird characters, like commas etc.
        @param str: string to clean up
        @retur: cleaned string
        """        
        s = self._re_clean_string.sub(' ', str)
        #s = s.replace('..', ' ')
        #s = s.replace('.', '')
        return s.strip()

    def preprocess(self, s):
        """
        @param s: string to process
        @return: processed string
        """
        #normalize 1: Remove multiple white spaces:
        s = self.clean(s).strip()

        # find long, lat data
        is_coord, long, lat = self.match_coord(s)
        if is_coord:
            return ["coord(%f,%f)" % (long, lat)]

        # with highest priority try it out as it is:
        self.__push_queue([s], prio=-10)
        self.__push_queue([self.title_words(s)], prio=9)

        # preprocess
        s = s.strip('.,;\-/\\:" ')
        s = self.remove_brackets(s)


        s = s.replace('!', ' ')
        s = s.replace(':)', '')
        s = s.replace(':-)', '')
        s = s.strip('.,;-/\\:" ')

        # if there is a comma, push the version split at the comma, then try '-' and then '/'
        if s.find(',') >= 0:
            self.__push_queue([self.clean(str) for str in s.split(',')], prio=-9, push_titled_version=True)
        elif s.find('-') >= 0:
            self.__push_queue([self.clean(str) for str in s.split('-')], prio=-8, push_titled_version=True)
        elif s.find('.') >= 0:
            self.__push_queue([self.clean(str) for str in s.split('.')], prio=-7, push_titled_version=True)
        elif s.find('/') >= 0:
            self.__push_queue([self.clean(str) for str in s.split('/')], prio=-6, push_titled_version=True)
        elif s.find('|') >= 0:
            self.__push_queue([self.clean(str) for str in s.split('|')], prio=-5, push_titled_version=True)
        elif s.find('\.') >= 0:
            self.__push_queue([self.clean(str) for str in s.split('.')], prio=-4, push_titled_version=True)
        elif s.find('&') >= 0:
            self.__push_queue([self.clean(str) for str in s.split('&')], prio=-3, push_titled_version=True)
        
        # push the version where every word is separated
        l = self._re_split_mult.split(s)
        self.__push_queue(l, prio=len(l), push_titled_version=True)

        l = [str.strip() for str in self._re_split.split(s.strip())]

        for item in l:
            if item != '':
                self.__push_queue([item], prio=len(item),
                                  push_titled_version=True)       # longer query lists get higher prio
        self.state = 1
        return True

    def next(self):
        """
        @return: the next variation
        """
        if self.queue.qsize() == 0:
            return []
        else:
            n = self.queue.get()[1]
            if self.state == 1:
                # start with variations of the string
                str = ' '.join(n)
                for v in self.vary(str.split(' ')):
                    self.__push_queue(v)
                    v_cap = map(self.title_words, v)
                    self.__push_queue(v, prio=10)       # low prio
                self.state = 2
            return n


if __name__ == '__main__':

    # this reads from stdin, it assumes a tab delimited input, the first column will be used

    try:
        for line in sys.stdin:
            line = line.strip()
            print '"%s":' % line
            p = PreProcess(line)
            while True:
                next = p.next()
                if next == []:
                    break
                print "   try ", next
    except KeyboardInterrupt:
        sys.stdout.flush()
        pass
