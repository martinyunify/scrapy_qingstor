# -*- coding: utf-8 -*-
import scrapy
from mirrorHttpRsyncTool.items import MirrorhttprsynctoolItem
from urllib.parse import urlparse

class LinuxrepoSpider(scrapy.Spider):
    name = 'LinuxRepo'
    allowed_domains= None

    def start_requests(self):
        url=self.settings.get('starturl')
        self.allowed_domains= [urlparse(url).netloc]
        yield scrapy.Request(url)

    def parse(self, response):
        items= response.css("a::text").extract()
        for item in items:
            if '..' in item:
                continue
            elif '/' not in item:
                yield MirrorhttprsynctoolItem(url=response.urljoin(item))
            else :
                subdir = response.urljoin(item)
                yield  scrapy.Request(subdir, callback=self.parse)
    
