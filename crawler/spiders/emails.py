# -*- coding: utf-8 -*-
import scrapy
from scrapy.linkextractors import LinkExtractor
from scrapy.spiders import CrawlSpider, Rule
from scrapy.exceptions import CloseSpider

import re
from urlparse import urlparse
import sys
import logging

class EmailsSpider(CrawlSpider):
    name = "emails" # required
    whitelist = ["text/plain", "text/html", "application/rss+xml"]
    custom_settings = {
        "ITEM_PIPELINES": {
            'crawler.pipelines.Web2EmailsPipeline': 300,
        }
    }
    # how many requests have I issued to this domain?
    n_requests = 0
    n_requests_max = 20

    # optionals 
    domain = None
    fckey = None
    hskey = None
    phone = None

    rules = (
        Rule(LinkExtractor(allow=r'.'), callback='parse_item', follow=True),
    )

    def parse_start_url(self, response):
        return self.get_email_addrs(response)

    def parse_item(self, response):
        logging.debug("Request number {}".format(self.n_requests))
        self.n_request_increment()
        if self.n_request_limit():
            raise CloseSpider("n_requests_max: {}".format(self.n_requests_max))
        return self.get_email_addrs(response)

    def n_request_increment(self):
        self.n_requests = self.n_requests + 1

    def n_request_limit(self):
        return self.n_requests_max < self.n_requests

    def get_email_addrs(self, response):
        logging.debug("Analysing document {}".format(response.url))
        pat = "\w+([-+.']\w+)*@\w+([-.]\w+)*\.\w+([-.]\w+)*"
        emails = re.finditer(pat, response.text)
        for e in emails:
            ret = {"email": e.group(0), "url": response.url }
            if EmailsSpider.phone:
                ret["phone"] = EmailsSpider.phone
            yield ret

    def __init__(self, start=None, fckey=None, hskey=None, phone=None, *args, **kwargs):
        super(EmailsSpider, self).__init__(*args, **kwargs)

        if start is None:
            sys.exit("Missing URL where to start crawling")
            
        # if fckey is None:
        #     sys.exit("Missing FullContact API key")

        # if hskey is None:
        #     sys.exit("Missing Hubspot API key")

        # EmailsSpider.fckey = fckey
        EmailsSpider.hskey = hskey
        EmailsSpider.phone = phone
        EmailsSpider.domain = urlparse(start).netloc.split(":")[0] # dodge the port number
        self.start_urls = [start]
        self.allowed_domains = [EmailsSpider.domain]
        logging.debug("Restricted to domain: {}".format(EmailsSpider.domain))
        # logging.debug("Hubspot API key: {}".format(EmailsSpider.hskey))
        # logging.debug("Phone number: {}".format(EmailsSpider.phone))
