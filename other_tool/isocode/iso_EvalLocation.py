#!/usr/bin/env python2.7
# -*- coding: utf-8 -*-
__author__ = 'markus'


import pickle
import operator
import sys
import os
from other_tool.isocode import Graph
from other_tool.isocode.PreProcess import PreProcess

file_path = (os.path.dirname(__file__))
sys.modules['Graph'] = Graph


class EvalLocation:
    """  a.k.a. The Location Engine
    """

    def __init__(self, graph_file, log_file=None):
        """
	    Evaluate the location
        @param graph_file: Filename of the .pkl file that has the graph
        """
        self.graph_file = graph_file

        # open the graph_file and unpacking it into the Graph object
        with open(graph_file, 'rb') as f:
            (self.G, self.countries) = pickle.load(f)
        
        # build the aliases dictionary to handle case insensitive location names
        self.G.build_aliases()
            
    def get_via_node(self, name_list):
        """
        This returns the list of via nodes in the dual queries.
            Ex: Boston, MA  --> via-nodes = ['MA']
        @param name_list: List of names in the query
        @return: List of via nodes' names
        """
        via_nodes = []
        for i in xrange(1, name_list.__len__()):
            via_nodes.append(name_list[i])
        return via_nodes

    def eval(self, input_str):
        """
        This performs the actual evaluation of location string
        @param s: String to evaluate
        @return: Estimated location (country)
        """
        # try:
        #     self.log.write("* %s\n" % input_str)
        # except Exception as e:
        #     self.log.write("* %s\n" % e)
        result_dict = dict()        # the dictionary contains ambiguous countries found
        result = None
        # pre processing the string
        p = PreProcess(input_str)
        while True:
            name_list = p.next()
            if name_list == []:
                break

            # removing empty names in the name_list
            original_name_list = name_list
            name_list = []
            for name in original_name_list:
                if (name.strip() != ''):
                    name_list.append(name)
            original_name_list = name_list
            # get the via nodes list 
            via_list = self.get_via_node(name_list)
            
            # cut off one member from the name_list 
            if name_list.__len__() != 1:
                name_list = name_list[:name_list.__len__() - 1]
 
            # self.log.write("+++name_list %s : " % name_list)
            # self.log.write("\n+++via_list %s : " % via_list)
            # self.log.write("   try %s : " % name_list)

            result = []
            length = []
            
            # try these names with the corresponding via nodes            
            for i in xrange(0, name_list.__len__()):
                if via_list == []:
                    path = self.G.shortest_path(name_list[i], "|0")[0]
                else:
                    path = self.G.shortest_path_via(name_list[i], "|0", via_list[i])[0]

                if path:     # pick first sublist in list, next to last entry, which is the country code
                    result.append(path[len(path) - 2][3:])
                    length.append(self.G.get_path_length())
                else:
                    result.append(None)
                    length.append(None)
            # dual queries cannot find anything --> try the single query version
            if all([i is None for i in result]):
                for from_name in original_name_list:
                    path = self.G.shortest_path(from_name, "|0")[0]
                    if path:     # pick first sublist in list, next to last entry, which is the country code
                        result.append(path[len(path) - 2][3:])
                        length.append(self.G.get_path_length())
                    else:
                        result.append(None)
                        length.append(None)                
            
            # save the length of all found paths
            for i in xrange(0,result.__len__()):
                if result[i] is not None:
                    if (result[i] in result_dict and result_dict[result[i]] > length[i]) or (result[i] not in result_dict):
                        result_dict[result[i]] = length[i]

            # handling results
            if all([i is None for i in result]):        # I got only None's
                # self.log.write("neg\n")
                continue
            uniq = list(set([x for x in result if x is not None]))
            if len(uniq) > 1:         # I got ambiguous results
                # self.log.write("ambig (%s)\n" % uniq)
                # disambiguate the result by getting the smallest length of path
                if any(result_dict):
                    result = sorted(result_dict.iteritems(), key=operator.itemgetter(1))[0][0]
                    s = " dis. ambig result: '%s'->%s\n" % (input_str, result)
                    # self.log.write(s)
                    return result

            else:
                # self.log.write("found %s\n" % uniq[0])
                result = uniq[0]
                s = "   result: '%s'->%s\n" % (input_str, result)
                # try:
                #     self.log.write(s)
                # except Exception as e:
                #     self.log.write(str(e))
                return result

        s = "   result: '%s'->%s\n" % (input_str, None)
        # try:
        #     self.log.write(s)
        # except Exception as e:
        #     self.log.write(str(e))
        return None


if __name__ == '__main__':

    # this is kind of the same as eval.py

    if len(sys.argv) < 2:
        sys.stderr.write("./EvalLocation.py <graph file>\n")
        sys.exit(1)

    graph_file = sys.argv[1]

    eval = EvalLocation(graph_file)

    try:
        for line in sys.stdin:
            line = line.strip()
            sys.stderr.write("%s\n" % line)
            e = eval.eval(line)
            print '%s\t%s' % (line, e)
    except KeyboardInterrupt:
        sys.stdout.flush()
