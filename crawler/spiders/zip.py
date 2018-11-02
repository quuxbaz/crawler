# -*- coding: utf-8 -*-
import scrapy
# from scrapy.linkextractors import LinkExtractor
from scrapy.spiders import CrawlSpider, Rule
# from scrapy.exceptions import CloseSpider

# import re
# from urlparse import urlparse
import sys
import logging

class ZipSpider(CrawlSpider):
    name = "zip" # required settings
    whitelist = ["text/plain", "text/html", "application/rss+xml"]
    custom_settings = {
        "ITEM_PIPELINES": {
            # 'crawler.pipelines.Web2EmailsPipeline': 300,
        }
    }

    # optionals
    # ...

    def stripstuff(self, ls):
        stripped = map(lambda s: s.strip(), ls)
        nonempty = filter(lambda s: len(s), stripped)
        return nonempty

    def make_items(self, response):
        # names = response.css(".nearby_affiliate .basic-row a span ::text").extract()
        names = response.xpath("//div[@class='nearby_affiliate']/div/h3/a/span/text()").extract()

        # addrs = response.css(".nearby_affiliate .address .address-line1 ::text").extract()
        addrs = response.xpath("//div[@class='nearby_affiliate']").css(".address .address-line1 ::text").extract()

        # localities = response.css(".nearby_affiliate .address .locality ::text").extract()
        localities = response.xpath("//div[@class='nearby_affiliate']").css(".address .locality ::text").extract()

        # states = response.css(".nearby_affiliate .address .administrative-area ::text").extract()
        states = response.xpath("//div[@class='nearby_affiliate']").css(".address .administrative-area ::text").extract()

        # zips = response.css(".nearby_affiliate .address .postal-code ::text").extract()
        zips = response.xpath("//div[@class='nearby_affiliate']").css(".address .postal-code ::text").extract()

        # countries = response.css(".nearby_affiliate .address .country ::text").extract()
        countries = response.xpath("//div[@class='nearby_affiliate']").css(".address .country ::text").extract()

        # url = response.css(".nearby_affiliate .basic-row div div a ::text").extract()
        urls = response.xpath("//div[@class='nearby_affiliate']/div/div/div[2]/a/text()").extract()

        # phone = response.css(".nearby_affiliate .basic-row div ::text")[19].extract().strip()
        phones = self.stripstuff(response.xpath("//div[@class='nearby_affiliate']/div/div/div[3]/text()").extract())
        emails = self.stripstuff(response.xpath("//div[@class='nearby_affiliate']/div/div/div[4]/text()").extract())

        def make_list(names):
            ret = []
            for i, name in enumerate(names):
                ret.append({
                    "name": name, 
                    "addr": addrs[i],
                    "city": localities[i],
                    "state": states[i],
                    "zip": zips[i],
                    "country": countries[i],
                    "url": urls[i],
                    "phone": phones[i],
                    "email": emails[i],
                })
            return ret

        return make_list(names)

    def parse_start_url(self, response):
        items = self.make_items(response)
        for i, item in enumerate(items):
            yield item

    # def parse_item(self, response):
    #     pass

    def __init__(self, start=None, *args, **kwargs):
        super(ZipSpider, self).__init__(*args, **kwargs)

        if start is None:
            sys.exit("Missing URL where to start crawling")
            
        self.start_urls = [start]
        logging.debug("HELLO: INIT.")
