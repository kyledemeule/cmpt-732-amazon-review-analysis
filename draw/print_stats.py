import sys, ast

def main():
    input_file = sys.argv[1]

    with open(input_file, 'r') as infile:
        res = []
        for line in infile:
            t = ast.literal_eval(line)
            if t[2] > 100:
                res.append(t)
    res = sorted(res, key=lambda t: t[1])

    c, s = [], []
    for r in res[:15]:
        c.append(r[0])
        s.append(r[1])
    print c
    print s
    c, s = [], []
    for r in res[-15:]:
        c.append(r[0])
        s.append(r[1])
    print c
    print s

if __name__ == "__main__":
    main()
