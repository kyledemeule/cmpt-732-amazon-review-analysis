import scrapy, re

stare = re.compile('^(\\d) stars represent (\\d+)% of rating$')

BASE_URL = "http://www.amazon.com/gp/product/"

EXAMPLE_ITEM = {
    "asin": "B00HN2C196",
    "total": {
        1: 123,
        2: 234,
        3: 345,
        4: 456,
        5: 567
    },
    "helpful": [
        3.0,
        1.0,
        5.0,
        5.0,
        5.0
    ]
}

class AmazonItemSpider(scrapy.Spider):
    name = 'amazon'

    def start_requests(self):
        asins = ["B00HN2C196", "B00405XZ3K"]
        for asin in asins:
            url = BASE_URL + asin
            request = scrapy.Request(url, callback=self.parse_page)
            request.meta["asin"] = asin
            yield request

    def parse_page(self, response):
        result = {}
        result["asin"] = response.meta["asin"]
        result["total"] = self.parse_total_reviews(response)
        result["helpful"] = self.parse_helpful_reviews(response)
        return result

    def parse_total_reviews(self, response):
        total = {}
        total_count = int(response.css("a.a-link-normal.a-text-normal.product-reviews-link::text").extract()[1].strip().replace(",", ""))

        for i in range(5):
            text = response.css("table#histogramTable tr.a-histogram-row")[i].css("td.a-nowrap a::attr(title)").extract_first()
            score, count = self.extract_scorecount(text, total_count)
            total[score] = count

        return total

    def extract_scorecount(self, text, total):
        m = stare.match(text)
        score = int(m.group(1))
        percent = float(m.group(2))
        count = total * (percent / 100.0)
        return score, round(count, 0)

    def parse_helpful_reviews(self, response):
        return {}