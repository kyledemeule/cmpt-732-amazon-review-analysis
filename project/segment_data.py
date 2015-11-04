import json, gzip, sys, time
import os.path

EACH_FILE_COUNT = 100000

def get_outfile_name(index):
    return os.path.join("dataset/", "amz-segment" + str(index) + ".json")

def main():
    path = sys.argv[1]
    g = gzip.open(path, 'r')
    segment = 1
    count = 0
    outfile = open(get_outfile_name(segment), 'w')
    start = time.time()
    for l in g:
        if count >= EACH_FILE_COUNT:
            total_time = round(time.time() - start, 2)
            print("Segment " + str(segment) + " done. Elapsed: " + str(total_time))
            segment += 1
            count = 0
            outfile.close()
            outfile = open(get_outfile_name(segment), 'w')
        outfile.write(l)
        count += 1
        if segment > 3:
            break
    outfile.close()

if __name__ == "__main__":
    main()