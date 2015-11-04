import json, gzip, sys

def main():
    path = sys.argv[1]
    g = gzip.open(path, 'r')
    with open('subset.json', 'w') as output_file:
        count = 0
        for l in g:
            if count > 100000:
                break
            output_file.write(l)
            count += 1


if __name__ == "__main__":
    main()