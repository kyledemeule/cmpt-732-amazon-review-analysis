import sys, matplotlib
import matplotlib.pyplot as pyplot

RESULTS = {
    1: 6757694,
    2: 4286384,
    3: 7073655,
    4: 15509942,
    5: 49208827
}

def main():
    pyplot.style.use('bmh')

    pyplot.bar(RESULTS.keys(), RESULTS.values(), color="#348ABD", label='Stuff', align='center')

    pyplot.xticks(RESULTS.keys())
    ax = pyplot.gca()
    ax.get_yaxis().get_major_formatter().set_scientific(False)
    pyplot.ylabel('Number of Reviews')
    pyplot.xlabel('Overall Score')


    pyplot.show()

if __name__ == "__main__":
    main()