import json, gzip, sys, ast

def main():
    path = sys.argv[1]
    g = gzip.open(path, 'r')
    with open('subset.json', 'w') as output_file:
        count = 0
        for l in g:
            if count > 10000:
                break
            d = ast.literal_eval(l)
            output_file.write(json.dumps(d) + "\n")
            count += 1


if __name__ == "__main__":
    main()