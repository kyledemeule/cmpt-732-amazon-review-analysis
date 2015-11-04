from pyspark import SparkConf, SparkContext
import sys, os.path

def process_raw_graph_line(line):
    comma_index = line.find(":")
    if comma_index == -1:
        return None
    else:
        node = int(line[:comma_index])
        edges = map(int, line[comma_index + 2:].split())
        return (node, edges)

# (1, ((None, 0), [3, 5]))
def make_nodes(current_node, current_path, reachable_nodes):
    results = []
    path_length = current_path[1]
    for reachable_node in reachable_nodes:
        new_node = (reachable_node, (current_node, path_length + 1))
        results.append(new_node)
    return results

def reduce_paths(path1, path2):
    if path1[1] < path2[1]:
        return path1
    else:
        return path2

def main():
    input_path = sys.argv[1]
    node_file = os.path.join(input_path, "links-simple-sorted.txt")
    output = sys.argv[2]
    source_node = int(sys.argv[3])
    destination_node = int(sys.argv[4])

    conf = SparkConf().setAppName('word count')
    sc = SparkContext(conf=conf)

    graph_info = sc.textFile(node_file).map(process_raw_graph_line).cache()
    paths = sc.parallelize([(source_node, (None, 0))])

    #for i in range(6):
    #    joined = paths.leftOuterJoin(graph_info)
    #    paths = joined.flatMap()
    #    paths.saveAsTextFile(output + '/iter-' + str(i))
    #    if we_seem_to_be_done:
    #        break
    # 
    #finalpath = ...
    #finalpath.saveAsTextFile(output + '/path')

    joined = paths.join(graph_info)
    one_iteration = paths.union(joined.flatMap(lambda (node, (path, reachable_nodes)): make_nodes(node, path, reachable_nodes)))
    reduced_iteration = one_iteration.reduceByKey(reduce_paths)
    reduced_iteration.coalesce(1).saveAsTextFile(output)

if __name__ == "__main__":
    main()