import numpy, sys
import matplotlib.pyplot as pyplot

def main():
    mode = sys.argv[1]
    # Data
    left_categories = [u'Brainydeal', u'SIB-CORP', u'BetterStuff LowerPrice', u'V4INK', u'Lenmar', u'Accessory Export', u'Franklin', u'Trademark', u'Group Vertical', u'South Shore', u'Powerwarehouse', u'Superb Choice', u'Bazic', u'Verizon', u'Tech Rover']
    left_values = [-0.11511784633434598, -0.09752998839333289, -0.09569105164121085, -0.09510342759040286, -0.07986000060300147, -0.07839196949182575, -0.07837681129363386, -0.07656627731890729, -0.07161878117779537, -0.06343505367108612, -0.060541956605228806, -0.05995770975635972, -0.05737561374332442, -0.056576433476476444, -0.054385884811255454]
    right_categories = [u'Sexy Hair', u'Fantasy Flight Games', u'Audio-Technica', u'Enzymatic', u'Beyblade', u'Futuro', u'LIONS GATE HOME ENT.', u'Premier-Pet-Products', u'Warner Home Video', u'Buena Vista Home Video', u'Warner', u'Eminence Organic Skin Care', u'MUSIC', u'Universal Studios', u'Wea2']
    right_values = [0.1901262708946354, 0.19090833662682005, 0.19196563251895613, 0.19363857372540846, 0.1945845957525999, 0.19515337035210042, 0.19781765704142948, 0.20261393477615147, 0.20359184139836992, 0.2069749534307141, 0.2188835424715936, 0.22609925943633108, 0.25558161135040397, 0.2617582284185454, 0.3053540459479765]
    y_pos = numpy.arange(len(left_categories))

    if mode == "neg":
        values, categories = left_values, left_categories
        color = "red"
        xlim = (-0.1, 0)
        nbins=6
        title = "Negative Brands"
        subplot = 122
    else:
        values, categories = right_values, right_categories
        color = "green"
        xlim = (0, 0.5)
        subplot = 121
        nbins=5
        title = "Positive Brands"

    pyplot.style.use('bmh')

    fig = pyplot.figure(figsize=(10,4))
    ax = fig.add_subplot(subplot)

    ax.barh(y_pos, values, align='center', alpha=0.6, color=color)
    pyplot.yticks(y_pos, categories)
    pyplot.title(title)

    ax2 = pyplot.gca()
    if mode == "neg":
        ax2.invert_yaxis()
    else:
        ax2.yaxis.tick_right()

    pyplot.locator_params(axis = 'x', nbins = nbins)

    ax2.set_xlim(xlim)
    ax2.axis('tight')
    pyplot.show()

if __name__ == "__main__":
    main()