__author__ = 'sunary'


import os
from other_tool.isocode.iso_EvalLocation import EvalLocation


path = os.path.dirname(os.path.abspath(__file__))
eval_model = EvalLocation(graph_file='/'.join([path, 'iso_location_pgm.pkl']))

print eval_model.eval('cx')
print eval_model.eval('SLC, UT')
print eval_model.eval('Madisonville, Louisiana')
print eval_model.eval('Florida & Wash-Baltimore area')
print eval_model.eval('DC')