# -*- coding: utf-8 -*-

BOT_NAME = 'web2csv'

SPIDER_MODULES = ['crawler.spiders']
NEWSPIDER_MODULE = 'crawler.spiders'

ITEM_PIPELINES = { }

DOWNLOADER_MIDDLEWARES = {
   'crawler.middlewares.WhiteListMiddleware': 0,
}

FEED_EXPORTERS = {
    'csv': 'crawler.exporters.HeadlessCsvItemExporter',
}

ROBOTSTXT_OBEY = False
DOWNLOAD_TIMEOUT = 5
