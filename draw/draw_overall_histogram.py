import sys, matplotlib
import matplotlib.pyplot as pyplot

def main():
    input_filepath = sys.argv[1]
    min_count = int(sys.argv[2]) if len(sys.argv) >= 3 else -1
    num_bins = int(sys.argv[3]) if len(sys.argv) >= 4 else 25

    overalls = []
    with open(input_filepath) as input_file:
        for line in input_file:
            pieces = line.split(", ")
            if len(pieces) >= 3:
                overall = float(pieces[1])
                count = int(pieces[2])
                if count > min_count:
                    overalls.append(overall)

    if False:
        pyplot.hist(overalls, num_bins)
    else:
        # buckets should be open except for last one, i.e. the first bucket is 1 <= val < 1.25
        bins=[1, 1.25, 1.75, 2.25, 2.75, 3.25, 3.75, 4.25, 4.75, 5.25]
        res = pyplot.hist(overalls, bins=bins)
        print res

    pyplot.show()

if __name__ == "__main__":
    main()