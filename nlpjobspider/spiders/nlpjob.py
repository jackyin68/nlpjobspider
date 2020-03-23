# -*- coding: utf-8 -*-
import scrapy
from scrapy.http import Request
from urllib import parse
from nlpjobspider.items import NlpjobspiderItem


class NlpjobSpider(scrapy.Spider):
    name = 'nlpjob'
    allowed_domains = ['nlpjob.com']
    start_urls = ['http://www.nlpjob.com/jobs/nlp/']

    def parse(self, response):
        urls = response.xpath('//*[@id="job-listings"]//span[@class="row-info"]//a/@href').getall()
        for url in urls:
            yield Request(url=parse.urljoin(response.url, url), callback=self.parse_details)

        next_url = response.xpath('//*[@id="job-listings"]/a[text()="»"]/@href').get()
        if next_url:
            yield Request(url=next_url, callback=self.parse)

    def parse_details(self, response):
        job_item = NlpjobspiderItem()
        title = list(response.xpath("//div[@id='job-details']/h2/text()").getall())[1].strip()
        # print(title)
        company = response.xpath('//div[@id="job-details"]/p/span[text()="于"]/following-sibling::*[1]/text()').get()
        # print(company)
        location = response.xpath('//div[@id="job-details"]/p/span[text()="in"]/following-sibling::*[1]/text()').get()
        if not location:
            location = ""
        # print(location)
        job_description = ("".join(response.xpath('//div[@id="job-description"]').extract())).strip()
        #print(job_description)
        job_item["title"] = title
        job_item["company"] = company
        job_item["location"] = location
        job_item["job_description"] = job_description
        yield job_item
