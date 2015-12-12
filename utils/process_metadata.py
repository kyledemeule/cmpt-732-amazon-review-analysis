import json, gzip, sys, ast

def main():
    path = sys.argv[1]
    outfile = sys.argv[2]
    g = gzip.open(path, 'r')
    with open(outfile, 'w') as output_file:
        count = 0
        for l in g:
            if count % 100000 == 0:
                print "done %i" % (count)
            d = ast.literal_eval(l)
            if "categories" in d:
                d["categories"] = d["categories"][0]
            output_file.write(json.dumps(d) + "\n")
            count += 1


if __name__ == "__main__":
    main()