# spark-bigData

 graph.tsv is a file in which each line is a triple of the form: SOURCE_NODE <TAB> DESTINATION_NODE <TAB> WEIGHT. 
 Note that source and destination nodes are integers, as are weights.
 
outdegree.py: For each node, compute the outdegree (number of outgoing edges) and output the (node, count) pairs in sorted order by node. 
 
weight.py: For each node, compute the sum of weights of incoming edges and output the (node, weight_sum) pairs in order sorted by node. 

pairs.py: For each node X, find a list of all other nodes Y such that there is an (X, Y) edge in the graph and a (Y, X) edge in the graph, and output the (X, [Y1, Y2, ..., Yn]) 
pairs in order sorted by X. I solved this by building two RDDs, one in which edge source nodes are keys and destination nodes are values, 
and one in which edge destination nodes are keys and source nodes are values. 

