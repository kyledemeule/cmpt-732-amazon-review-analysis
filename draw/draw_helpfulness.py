import sys, matplotlib
import matplotlib.pyplot as pyplot

RESULTS_TOTAL = {
    1: 5949,
    2: 8709,
    3: 9971,
    4: 22458,
    5: 79105
}

RESULTS_HELPFUL = {
    1: 568,
    2: 926,
    3: 933,
    4: 2258,
    5: 7424
}

def main():
    total_total = float(sum(RESULTS_TOTAL.values()))
    total_percent = dict([(s, RESULTS_TOTAL[s] / total_total) for s in RESULTS_TOTAL])
    helpful_total = float(sum(RESULTS_HELPFUL.values()))
    helpful_percent = dict([(s, RESULTS_HELPFUL[s] / helpful_total) for s in RESULTS_HELPFUL])

    pyplot.style.use('bmh')

    pyplot.plot(total_percent.keys(), total_percent.values(), '-', alpha=1, label="Total")
    pyplot.plot(helpful_percent.keys(), helpful_percent.values(), '-', alpha=0.7, label="Helpful", color="orange")

    ax = pyplot.gca()
    ax.set_xticks(RESULTS_TOTAL.keys())
    ax.set_xticklabels(map(lambda i: "%i Star" % (i), RESULTS_TOTAL.keys()))

    pyplot.ylabel('Percent of Reviews')
    pyplot.legend(prop={'size': 15}, loc=2)

    pyplot.show()

if __name__ == "__main__":
    main()