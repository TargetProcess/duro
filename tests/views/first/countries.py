import csv
import sys


def process(data):
    header = data[0]
    sums = [sum(int(row[i]) for i in range(len(header))) for row in data[1:]]

    return [header, sums]


if __name__ == "__main__":
    with open(sys.argv[1]) as csvin:
        data = list(csv.reader(csvin))

    processed = process(data)

    with open(sys.argv[2], "w") as csvout:
        writer = csv.writer(csvout)
        writer.writerows(processed)
