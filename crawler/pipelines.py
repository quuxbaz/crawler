# -*- coding: utf-8 -*-
from scrapy.exceptions import DropItem
from scrapy.exceptions import CloseSpider
from crawler.spiders.emails import EmailsSpider

import logging

import json
import requests
import time

def json_deep_get(js, key, default):
    """
    JSON [Key] String -> String
    It takes a json JS, a list [key1, key2, ..., keyN] and produces
    the value JS[key1][key2]...[keyN] if it exists.  If it doesn't
    exist, produce the default parameter.

    Examples: 
    json_deep_get({}, [], "hello") = ValueError()
    json_deep_get({"x": {1: {"z": "jesus"}}}, ['x', 1, 'z'], "hello") = "jesus"
    json_deep_get({"x": {1: {"z": "jesus"}}}, ['y'], "hello") = "hello"
    """

    if key == []:
        raise ValueError("key list cannot be empty")

    if key[0] not in js:
        return default
    elif not isinstance(js, dict):
        raise TypeError
    else:
        if len(key) == 1:
            return js[key[0]]
        else:
            return json_deep_get(js[key[0]], key[1:], default)

def json_with_pair(ls, key, val):
    """
    [JSON] Key Val -> JSON
    Takes a list of json LS, a pair KEY, VAL and produces the first
    JSON that has the pair KEY: VAL.  If there is no such json, {}
    is produced.  The empty dictionary is convenient for combining
    this function with json_deep_get(). I search one level only.

    Examples:
    json_with_pair([{"typeid": "twitter"},{"typeid": "gravatar"}], "typeid", "twitter") = {"typeid": "twitter"}
    json_with_pair([], "whatever", "anything") = {}
    """
    # filter(ls, lambda js: return js if key in js and js[key] == val)
    for js in ls:
        if key in js and js[key] == val:
            return js
    return {}

def get_organization_item(js, item):
    orgs = json_deep_get(js, ["organizations"], [])
    if not orgs:
        return ""
    return json_deep_get(orgs[0], [item], "")

def get_social_url(js, typeId):
    return json_deep_get(json_with_pair(json_deep_get(js, ["socialProfiles"], []), "typeId",typeId), ["url"], "")

def fullcontact_to_hubspot(item):
    """ ITEM --> JSON.  Takes an ITEM and produces a JSON Hubspot-ready Contact."""
    if "person" in item: 
        src = item["person"]
        ls = [
                {"property": "fullcontactjson", "value": json.dumps(src)},
                {"property": "source", "value": item["url"]},
                {"property": "email", "value": item["email"]},
                {"property": "firstname", "value": 
                 json_deep_get(src, ["contactInfo", "givenName"], "")},
                {"property": "lastname", "value": 
                 json_deep_get(src, ["contactInfo", "familyName"], "")},
                {"property": "website", "value": 
                 json_deep_get(src, ["socialProfiles", 0, "url"], "")},
                {"property": "city", "value": 
                 json_deep_get(src, ["demographics", "locationDeduced", "city", "name"], "")},
                {"property": "state", "value": 
                 json_deep_get(src, ["demographics", "locationDeduced", "state", "name"], "")},
                {"property": "company", "value": get_organization_item(src, "name")},
                {"property": "jobtitle", "value": get_organization_item(src, "title")},
                {"property": "twitterurl", "value": get_social_url(src, "twitter")},
                {"property": "linkedinurl", "value": get_social_url(src, "linkedin")},
            ]
    else:
        ls = [
            {"property": "email", "value": item["email"]},
            {"property": "source", "value": item["url"]},
        ]
    
    if "phone" in item:
        # Watch out.  This case isn't the same as the above ones.  We
        # may have scraped this contact before and we may be scraping
        # again WITHOUT a phone number.  If we used json_deep_get, we
        # would end up writing a blank phone on top of an existing
        # phone number.  We'd lose the information.
        ls.append({"property": "phone_scraped", "value": item["phone"]})

    return {"properties": filter(lambda js: js["value"], ls)}

class Web2EmailsPipeline(object):
    seen = set()

    def process_item(self, item, spider):
        if not item["email"].upper() in self.seen:
            self.seen.add(item["email"].upper())
            return item
        raise DropItem("Address {} already harvested".format(item["email"]))

class SeenBeforePipeline(object):
    seen = set()

    def process_item(self, item, spider):
        if not item["id"].upper() in self.seen:
            self.seen.add(item["id"].upper())
            return item
        raise DropItem("Item has been seen before: {}".format(item["id"]))

class FullContactPipeline(object):
    apikey = EmailsSpider.fckey
    url = "https://api.fullcontact.com/v2/person.json"
    later = set()

    def whois(self, **kwargs):
        if 'apiKey' not in kwargs:
            kwargs['apiKey'] = self.apikey
        r = requests.get(self.url, params=kwargs)
        return json.loads(r.text)

    def process_item(self, item, spider):
        http = self.whois(email=item["email"])
        if http["status"] == 202:
            FullContactPipeline.later.add(item["email"])
        elif http["status"] == 200:
            logging.info("FullContact: person: {}".format(json.dumps(json.dumps(http))))
            item["person"] = http
        return item

    def reprocess_item(self, email):
        http = self.whois(email=email)
        item = {"email": email}
        if http["status"] == 202:
            logging.warning("We got a ask-later again!? (So slow.) I give up.")
        elif http["status"] == 200:
            logging.info("We do have a profile on {}".format(email))
            logging.info("FullContact: person: {}".format(json.dumps(json.dumps(http))))
            h = HubspotPipeline()
            h.update_contact(item)
        elif http["status"] == 404:
            logging.info("We found nothing on {}".format(email))

    def close_spider(self, spider):
        for email in self.later:
            logging.info("Trying {} again".format(email))
            ## should be reprocessed, but our processing scheme is not
            ## general enough at the moment
            self.reprocess_item(email)

class HubspotPipeline(object):
    apikey = EmailsSpider.hskey
    host = "api.hubapi.com"
    list_id = None

    def create_property(self, js):
        """
        JSON -> HttpCode
        Creates a property at Hubspot and produces the HttpCode of the
        operation, that is, 409 if it conflicts, 200 if it was created
        just fine.
        """
        uri = "https://{}/properties/v1/contacts/properties?hapikey={}".format(self.host, self.apikey)
        r = self.hubspot_post(uri, js)
        if r.status_code != 200 and r.status_code != 409:
            raise CloseSpider(r.content)
        return r.status_code
    
    def create_list(self, name):
        uri = "https://{}/contacts/v1/lists?hapikey={}".format(self.host, self.apikey)
        js = {"name": name}
        r = self.hubspot_post(uri, js)
        if r.status_code == 200:
            return r.json()["listId"]
        raise CloseSpider(r.content)

    def create_or_update_contact(self, item):
        uri = "https://{}/contacts/v1/contact/?hapikey={}".format(self.host, self.apikey)
        js = fullcontact_to_hubspot(item)
        r = self.hubspot_post(uri, js)
        if r.status_code == 200:
            return r.json()["vid"]
        if r.status_code == 409:
            # Contact already exists.
            return self.update_contact(item)
        elif r.status_code == 400:
            logging.error(r.content)
        else:
            raise CloseSpider(r.content)        

    def update_contact(self, item):
        uri = "https://{}/contacts/v1/contact/email/{}/profile?hapikey={}".format(
            self.host, item["email"], self.apikey
        )
        js = fullcontact_to_hubspot(item)
        r = self.hubspot_post(uri, js)
        if r.status_code == 204:
            logging.info("Hubspot: updated contact {}".format(item["email"]))
            # ``HTTP/1.1 204 No Content'' -- no information given
            return r.status_code
        else:
            raise CloseSpider(r.content)

    def subscribe_contact_to_list(self, item, list_id):
        uri = "https://{}/contacts/v1/lists/{}/add?hapikey={}".format(self.host, self.list_id, self.apikey)
        js = {"emails": [ item["email"] ]}
        r = self.hubspot_post(uri, js)
        if r.status_code == 200:
            logging.info("Hubspot: added {} to list {}".format(item["email"], self.list_id))
            return 200
        raise CloseSpider(r.content)

    def hubspot_post(self, uri, js):
        headers = {"Content-Type": "application/json"}
        logging.debug("Hubspot: HTTP POST: {}".format(json.dumps(js)))
        r = requests.post(uri, headers=headers, data=json.dumps(js))
        time.sleep(0.15)
        logging.info("Hubspot: {}: {}".format(r.status_code, r.content))
        return r

    def process_item(self, item, spider):
        ## Create contact or update it
        self.create_or_update_contact(item)
        self.subscribe_contact_to_list(item, self.list_id)
        return item

    def open_spider(self, spider):
        #Make sure we've our properties in place
        self.create_property({ 
            "name": "fullcontactjson",
            "label": "FullContact JSON",
            "description": "Stores the entire FullContact JSON",
            "groupName": "contactinformation",
            "type": "string",
            "fieldType": "textarea",
            "formField": True,
            "displayOrder": -1
        })

        self.create_property({ 
            "name": "source",
            "label": "Where was this e-mail address found?",
            "description": "Stores the URL from where the contact was scraped",
            "groupName": "contactinformation",
            "type": "string",
            "fieldType": "text",
            "formField": True,
            "displayOrder": -1
        })

        self.create_property({ 
            "name": "phone_scraped",
            "label": "Phone (scraped)",
            "description": "Phone number passed in through the crawler's command line.",
            "groupName": "contactinformation",
            "type": "string",
            "fieldType": "text",
            "formField": True,
            "displayOrder": -1
        })

        self.create_property({ 
            "name": "twitterurl",
            "label": "Twitter URL",
            "description": "Stores the Twitter URL",
            "groupName": "socialmediainformation",
            "type": "string",
            "fieldType": "text",
            "formField": True,
            "displayOrder": -1
        })

        self.create_property({ 
            "name": "linkedinurl",
            "label": "LinkedIn URL",
            "description": "Stores the LinkedIn URL",
            "groupName": "socialmediainformation",
            "type": "string",
            "fieldType": "text",
            "formField": True,
            "displayOrder": -1
        })

        ## Create a list just for this instance run.
        self.list_id = self.create_list("{} {}".format(spider.domain, time.strftime("%Y-%m-%d %H:%M:%S")))
