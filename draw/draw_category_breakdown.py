import numpy, sys
import matplotlib.pyplot as pyplot

def main():
    mode = sys.argv[1]
    # Data
    left_categories = [u'Data Cables', u'Laser Printer Toner', u'Travel Chargers', u'Printer Ink & Toner', u'Video Projector Accessories', u'Lamps', u'AC Adapters', u'Laser Printers', u'Car Chargers', u'Inkjet Printer Ink']
    left_values = [-0.09541302262844703, -0.08208582689631028, -0.06613682188676605, -0.04971448014236662, -0.049647327841092964, -0.04797892938586791, -0.042873560937951326, -0.041004772819955966, -0.03685208958946596, -0.033316512065950536]
    right_categories = [u'Religion & Spirituality', u'Diets & Weight Loss', u'Industries & Professions', u'Business & Money', u'Diseases & Physical Ailments', u'Self-Help', u'Schools & Teaching', u'Small Business & Entrepreneurship', u'Motivational', u'Memoirs']
    right_values = [0.351410585592364, 0.36405184464791285, 0.36806578032247617, 0.37570708124242264, 0.37576967268549516, 0.3782064534844909, 0.39010647626421, 0.40247300394630525, 0.437452854679377, 0.4469313189049694]
    y_pos = numpy.arange(len(left_categories))

    if mode == "neg":
        values, categories = left_values, left_categories
        color = "red"
        xlim = (-0.1, 0)
        nbins=6
        title = "Negative Categories"
        subplot = 122
    else:
        values, categories = right_values, right_categories
        color = "green"
        xlim = (0, 0.5)
        subplot = 121
        nbins=5
        title = "Positive Categories"

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