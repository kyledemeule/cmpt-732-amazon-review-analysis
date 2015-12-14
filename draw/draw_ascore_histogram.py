import sys, matplotlib, ast
import matplotlib.pyplot as pyplot

def main():
    input_filepath = sys.argv[1]
    min_count = int(sys.argv[2]) if len(sys.argv) >= 3 else -1
    num_bins = int(sys.argv[3]) if len(sys.argv) >= 4 else 25

    overall_normal = []
    overall_weighted = []
    with open(input_filepath) as input_file:
        for line in input_file:
            pieces = line[line.find("'"):-2].split(",")
            if len(pieces) >= 4:
                overall = float(pieces[1])
                weighted = float(pieces[2])
                count = int(pieces[len(pieces) - 1])
                if count > min_count:
                    overall_normal.append(overall)
                    overall_weighted.append(weighted)

    #(u'B00006GNP9', 4.617647058823529, 4.571428571428571, 34)
    pyplot.style.use('bmh')

    if False:
        pyplot.hist(overall_normal, num_bins, alpha=0.8, label="Normal")
        pyplot.hist(overall_weighted, num_bins, alpha=0.8, label="Weighted")
    else:
        # buckets should be open except for last one, i.e. the first bucket is 1 <= val < 1.25
        bins=[1, 1.25, 1.75, 2.25, 2.75, 3.25, 3.75, 4.25, 4.75, 5.25]
        pyplot.hist(overall_normal, bins=bins, alpha=0.5, color="black", label="Normal")
        pyplot.hist(overall_weighted, bins=bins, alpha=0.5, color="green", label="Weighted")
        pyplot.ylabel('Count')
        pyplot.xlabel('Product Averages (products with > 5 reviews)')
        pyplot.legend(prop={'size': 10}, loc=2)
        #res = pyplot.hist(overalls, bins=bins)
        #print res

    pyplot.show()

if __name__ == "__main__":
    main()