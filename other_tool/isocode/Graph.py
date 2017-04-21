#!/usr/bin/env python2.7
# -*- coding: utf-8 -*-
__author__ = 'markus'


import igraph


class GraphException(Exception):
    pass


class Graph:
    """ the location graph """

    def __init__(self, directed=True):
        self.__directed = directed
        self.__g = igraph.Graph(directed=directed)
        self.__vertices = dict()       # lookup for vertices
        self.__path_length = 0
        
    def is_directed(self):
        return self.__directed
        
    def get_path_length(self):
        return self.__path_length
        
    def build_aliases(self):
        """ build the aliases dictionary to handle case insensitive location names """
        self.__alias = dict()
        for k in self.__vertices.iterkeys():
            if not self.__alias.has_key(k.lower()):
                 self.__alias[k.lower()] = k
            
    def add_vertex(self, name, raise_exception=False):
        """ adds a new vertex, returns the id, if a vertex with that name already exists, nothing is done """
        if self.__vertices.has_key(name):
            if raise_exception:
                raise GraphException("Error adding vertex '%s': Vertex already exists." % name)
            else:
                return self.__vertices[name]
        else:
            self.__g.add_vertex(1)
            n = self.__g.vcount() - 1
            self.__vertices[name] = n
            self.__g.vs[n]['name'] = name
            return n

    def get_vertex(self, name):
        if self.__alias.has_key(name.lower()):
            return self.__vertices[self.__alias[name.lower()]]
        else:
            raise GraphException("Error getting vertex '%s'. Vertex does not exist." % name)

    def get_child_vertex(self, name):
        """ get the child vertices of the vertex @name """
        if self.__alias.has_key(name.lower()):
            return self.__g.neighbors(self.__vertices[self.__alias[name.lower()]], igraph.OUT)
        else:
            raise GraphException("Error getting vertex '%s'. Vertex does not exist." % name)

    def add_edge(self, from_name, to_name, weight=1, raise_exception=True):
        """ adds a new vertex, returns the id, if a vertex with that name already exists, nothing is done """
        skip_check = not self.__vertices.has_key(from_name) or not self.__vertices.has_key(to_name)
        n1 = self.add_vertex(from_name, raise_exception=False)
        n2 = self.add_vertex(to_name, raise_exception=False)
        is_dupl = False
        if not skip_check:
            # check edge doesn't already exist
            v = self.__g.vs[n1]
            is_dupl = (n2 in [s.index for s in v.successors()])
            if is_dupl and raise_exception:
                raise GraphException("Error adding edge '%s'->'%s': Edge already exists." % (from_name, to_name))
        if not is_dupl:
            self.__g.add_edges([(n1, n2)])
        e = self.__g.es[self.__g.ecount() - 1]
        e['w'] = weight

    def calculate_path_length(self, path):
        """
        Calculating the length of the found path
            Edge structure in igraph:
                        (A) ---3--->(B) ---2--->(D)
                        (A) ---7--->(C)
                edge[A,B]['w'] = [3,2]
                edge[A,C]['w'] = [7,0]
                edge[B,D]['w'] = [2,0]
        """
        length = 0
        if path != [[]]:
            for i in xrange(0,path[0].__len__() - 1):
                length += self.__g.es[path[0][i],path[0][i + 1]]['w'][0]
        return length

    def shortest_path(self, from_name, to_name, return_id=False):
        """ calculate the shortest path, if return_id is True, return the ids, otherwise return the vertex names """
        self.__path_length = 0
        try:
            n1 = self.get_vertex(from_name)
            n2 = self.get_vertex(to_name)

        except GraphException:
            return [[]]
        try:
            path = self.__g.get_shortest_paths(n1, n2, weights='w')
            self.__path_length = self.calculate_path_length(path)
        except:
            return [[]]
        if return_id:
            return path
        else:
            return [self.__g.vs[i]['name'] for i in path]

    def shortest_path_via(self, from_name, to_name, via_name, return_id=False):
        """ calculate the shortest path which goes through a child node of a certain node, 
                if @return_id is True, return the ids, 
                otherwise return the vertex names 
        """
        # save the max length of the shortest path and the via node
        max = 0
        max_via_node = 0
        self.__path_length = 0
        try:
            n1 = self.get_vertex(from_name)
            n2 = self.get_vertex(to_name)
            # get the list of via nodes
            via_nodes = self.get_child_vertex(via_name)

        except GraphException:
            return [[]]
        try:

            """
            via nodes are not interconnected, therefore, the shortest path from S to D that go through these via nodes is
            the sum of the shortest path from S to the via nodes and the shortest path from the via nodes to D                                
            """
            for via_node in via_nodes:
                length = 0
                try:
                    length += self.calculate_path_length(self.__g.get_shortest_paths(n1, via_node, weights='w'))
                    if length != 0:
                        length += self.calculate_path_length(self.__g.get_shortest_paths(via_node, n2, weights='w'))
                    # save the highest score path
                    if length > max:
                        max = length
                        max_via_node = via_node
                        length = 0
                except:
                    path = [[]]
 
            if max != 0:
                self.__path_length = max
                first_path = self.__g.get_shortest_paths(n1, max_via_node, weights='w')[0]
                second_path = self.__g.get_shortest_paths(max_via_node, n2, weights='w')[0]
                path = []
                path.append(first_path + second_path[1:])
            else:
                return [[]]
        except:
            return [[]]
        if return_id:
            return path
        else:
            return [self.__g.vs[i]['name'] for i in path]

    def get(self):
        """ returns the igraph.Graph """
        return self.__g

    def __str__(self):
        return str(self.__g)

    def make_dot(self, fn=None):
        """ Make dot string. If fn is given, save to file """
        if self.is_directed():
            s = 'digraph G {\n'
            for v in self.__g.vs():
                children = v.successors()
                if len(children) == 0:
                    s += '"%s" [shape=box];\n' % v['name']
                for c in children:
                    s += '"%s"->"%s";\n' % (v['name'], c['name'])
            s += "}"
        else:
            lookup = {}
            s = 'graph G {\n'
            for v in self.__g.vs():
                v_name = v['name']
                children = v.successors()
                if len(children) == 0:
                    s += '"%s" [shape=box];\n' % v_name
                for c in children:
                    c_name = c['name']
                    if not lookup.has_key((v_name, c_name)) and not lookup.has_key((c_name, v_name)):
                        s += '"%s"--"%s";\n' % (v_name, c_name)
                        lookup[(v_name, c_name)] = 1
            s += "}"
        if fn is not None:
            with open(fn, 'w') as f:
                f.write(s)
        return s


if __name__ == '__main__':
    import unittest

    class Test(unittest.TestCase):
        def setUp(self):
            pass

        def test1(self):
            g = Graph(directed=False)
            g.add_edge('a', 'b')
            g.add_edge('b', 'c')
            g.add_edge('c', 'd')
            g.add_edge('a', 'c', 10)
            path = g.shortest_path('a', 'd')
            self.assertTrue(path == [['a', 'b', 'c', 'd']])

        def test2(self):
            g = Graph(directed=True)
            g.add_edge('a', 'b',3)
            g.add_edge('b', 'c')
            g.add_edge('c', 'd',5)
            g.add_edge('a', 'c', 10)
            path = g.shortest_path('a', 'd')
            print g.get().es[0,3]['w']
            self.assertTrue(path == [['a', 'b', 'c', 'd']])

        def test3(self):
            g = Graph(directed=False)
            g.add_edge('a', 'b')
            self.assertEqual(g.get_vertex('b'), 1)
            self.assertRaises(Exception, g.add_edge, ('a', 'b'))
            g.add_edge('a', 'b', raise_exception=False)
            self.assertRaises(Exception, g.get_vertex, ('c'))

        def test4(self):
            g = Graph(directed=True)
            g.add_edge('a', 'b')
            self.assertRaises(Exception, g.add_edge, ('a', 'b'))
            g.add_edge('a', 'b', raise_exception=False)

    unittest.main()
