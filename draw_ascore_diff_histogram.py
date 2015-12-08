import sys, matplotlib
import matplotlib.pyplot as pyplot

def main():
    input_filepath = sys.argv[1]
    min_count = int(sys.argv[2]) if len(sys.argv) >= 3 else -1

    diffs = []
    with open(input_filepath) as input_file:
        for line in input_file:
            pieces = line[line.find("'"):-2].split(",")
            if len(pieces) >= 4:
                overall = float(pieces[1])
                weighted = float(pieces[2])
                count = int(pieces[len(pieces) - 1])
                if count > min_count:
                    diffs.append(weighted - overall)

    #(u'B00006GNP9', 4.617647058823529, 4.571428571428571, 34)
    pyplot.style.use('bmh')

    pyplot.hist(diffs, bins=100)
    pyplot.axis([-1.5, 1.5, 0, 300000])
    pyplot.ylabel('Count')
    pyplot.xlabel('Score change after weighting (products with > 5 reviews)')

    pyplot.show()

if __name__ == "__main__":
    main()