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


def main():
    input_path = sys.argv[1]
    node_file = os.path.join(input_path, "links-simple-sorted.txt")
    output = sys.argv[2]
    source_node = sys.argv[3]
    destination_node = sys.argv[4]

    conf = SparkConf().setAppName('word count')
    sc = SparkContext(conf=conf)

    graph_info = sc.textFile(node_file).map(process_raw_graph_line).cache()
    graph_info.coalesce(1).saveAsTextFile(output)

if __name__ == "__main__":
    main()