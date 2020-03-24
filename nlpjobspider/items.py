# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy
from nlpjobspider.models.estype import NlpJobType
from w3lib.html import remove_tags
from elasticsearch_dsl.connections import connections
import redis

redis_cli = redis.StrictRedis()
es = connections.create_connection(NlpJobType._doc_type.using)


def gen_suggests(index, info_tuple):
    used_words = set()
    suggests = []
    for text, weight in info_tuple:
        if text:
            words = es.indices.analyze(index=index, analyzer="ik_max_word", params={'filter': ["lowercase"]}, body=text)
            analyzed_words = set([r["token"] for r in words["tokens"] if len(r["token"]) > 1])
            new_words = analyzed_words - used_words
        else:
            new_words = set()

        if new_words:
            suggests.append({"input": list(new_words), "weight": weight})
            used_words = used_words | new_words

    return suggests


class NlpjobspiderItem(scrapy.Item):
    title = scrapy.Field()
    company = scrapy.Field()
    location = scrapy.Field()
    job_description = scrapy.Field()
    url = scrapy.Field()

    def save_to_es(self):
        nlpjob = NlpJobType()
        nlpjob.title = self['title']
        nlpjob.company = self['company']
        nlpjob.location = self['location']
        nlpjob.url = self['url']
        nlpjob.job_description = "".join(remove_tags(self['job_description']))
        nlpjob.suggest = gen_suggests(NlpJobType._doc_type.index,
                                      ((nlpjob.title, 10), (nlpjob.company, 10), (nlpjob.location, 8)))
        nlpjob.save()
        redis_cli.incr("nlpjob_count")
        return
