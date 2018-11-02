# -*- coding: utf-8 -*-
from scrapy.item import Item, Field

class Web2EmailsItem(Item):
    email = Field()

    url = Field()
    project = Field()
    spider = Field()
    server = Field()
    date = Field()

