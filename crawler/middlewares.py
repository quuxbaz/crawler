from scrapy.exceptions import IgnoreRequest
import logging
import re

class WhiteListMiddleware(object):
    """Implements a download white list."""

    @staticmethod
    def is_content_type_okay(whitelist, content_type):
        for r in whitelist:
            if re.search(r, content_type):
                return True
        return False

    def process_response(self, request, response, spider):
        """Only allow HTTP response types that that match the given white list
        of regular expressions. Each spider must define a whitelist
        iterable containing regular expressions whose content type's
        the spider wishes to download.
        """

        whitelist = getattr(spider, "whitelist", None)
        if not whitelist:
            return response

        content_type = response.headers.get('content-type', None)
        if not content_type:
            logging.info("spider {}: ignored: {} does not contain a content-type header".format(spider.name, response.url))
            raise IgnoreRequest()

        if self.is_content_type_okay(whitelist, content_type):
            return response

        logging.info("spider {}: ignored: {} has type {}, which was not whitelisted".format(spider.name, response.url, content_type))
        raise IgnoreRequest()
