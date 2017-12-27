# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.scrapy.org/en/latest/topics/item-pipeline.html
from qingstor.sdk.service.qingstor import QingStor
from qingstor.sdk.config import Config
import requests
from urllib.parse import urlparse
import logging
import sys

class MirrorhttprsynctoolPipeline(object):

    TOO_LONG = 1024*1024*64

    def __init__(self,qs_access_key,qs_secret_key,qs_zone,qs_bucket_name,qs_bucket_prefix):
        self.qs_asscess_key = qs_access_key
        self.qs_secret_key = qs_secret_key
        self.qs_zone = qs_zone
        self.qs_bucket_name = qs_bucket_name
        self.qs_bucket_prefix = qs_bucket_prefix        

    @classmethod
    def from_crawler(cls, crawler):
        return cls(
            qs_access_key = crawler.settings.get("QS_ACCESS_KEY"),
            qs_secret_key = crawler.settings.get("QS_SECRET_KEY"),
            qs_zone = crawler.settings.get("QS_ZONE"),
            qs_bucket_name = crawler.settings.get("QS_BUCKET_NAME"),
            qs_bucket_prefix = crawler.settings.get("QS_BUCKET_PREFIX")
        )

    def process_item(self, item, spider):
        url= item['url']
        key= ''
        # remove / in prefix
        if self.qs_bucket_prefix is not None and len(self.qs_bucket_prefix) >0:
            key = self.qs_bucket_prefix+urlparse(url).path
        else :
            key = urlparse(url).path[1:]

        if not self.is_timestamp_equal(url,key):
            logging.info("%s updated. download new version from repo",key)
            try:
                with self.session.get(url, stream=True) as response:
                    response.raise_for_status()
                    if int(response.headers['content-length']) < self.TOO_LONG:
                        self.bucket.put_object(key,body=response.content)
                        logging.log(logging.INFO,"upload %s" % key)
                    else:
                        uploadJobinfo=self.bucket.initiate_multipart_upload(key)
                        uploadid= uploadJobinfo['upload_id']
                        partNo = 0
                        partList= []
                        # Throw an error for bad status codes
                        for block in response.iter_content(chunk_size=self.TOO_LONG):
                            output = self.bucket.upload_multipart(key,upload_id=uploadid,part_number=partNo,body=block)
                            logging.info("upload: %s,id:%s,part %d,status: %d" ,key,uploadid,partNo,output.status_code)
                            partNo +=1
                            partList.append(partNo)
                        self.bucket.complete_multipart_upload(key,upload_id=uploadid,object_parts=[{"part_number":x} for x in partList])
                        logging.info("uploaded: %s" ,key)

            except:
                logging.exception("Failed to download %s Unexpected error.",url)
        return item
    
    def is_timestamp_equal(self,url,key):
        objStatus=self.bucket.head_object(key)
        if objStatus.status_code == 200 :
            with self.session.head(url) as response:
                logging.info("key: %s source length:%s target length %s modified %s source %s",key,response.headers['Content-Length'],objStatus.headers['Content-Length'],response.headers['Last-Modified'],objStatus.headers['Last-Modified'])
                return objStatus.headers['Content-Length'] == response.headers['Content-Length'] #and objStatus.headers['Last-Modified']== response.headers['Last-Modified']
        return False
                


    def open_spider(self, spider):
        self.config= Config(self.qs_asscess_key,self.qs_secret_key)
        self.qingstor = QingStor(self.config)
        self.bucket = self.qingstor.Bucket(self.qs_bucket_name,self.qs_zone)
        self.session = requests.Session()


        