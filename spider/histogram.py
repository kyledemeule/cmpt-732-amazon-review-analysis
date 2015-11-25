import ast
from collections import Counter

def main():
    total = Counter({'1': 0, '2': 0, '3': 0, '4': 0, '5': 0})
    helpful = Counter({'1': 0, '2': 0, '3': 0, '4': 0, '5': 0})

    with open("amz.json") as input_file:
        for line in input_file:
            line = line.strip()
            if line[0] == "[":
                line = line[1:]
            if line[-1] == "," or line[-1] == "]":
                line = line[:-1]
            line = line.replace("\n", "")

            json = ast.literal_eval(line)

            total.update(Counter(json["total"]))
            helpful.update(Counter(json["helpful"]))

    print("Total:")
    print(total)
    print("Helpful:")
    print(helpful)

if __name__ == "__main__":
    main()