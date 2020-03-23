# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html
import pymysql
from pymysql import cursors
from twisted.enterprise import adbapi
import copy

from nlpjobspider.models.estype import NlpJobType
from w3lib.html import remove_tags


class ElasticsearchPipeline(object):
    def process_item(self, item, spider):
        item.save_to_es()
        return item


class NlpjobspiderPipeline(object):
    def process_item(self, item, spider):
        return item


class MysqlJobsPipeline(object):
    def __init__(self):
        dbparams = {
            'host': '127.0.0.1',
            'port': 3306,
            'user': 'root',
            'password': 'root',
            'database': 'nlpjob',
            'charset': 'utf8',
        }
        self.conn = pymysql.connect(**dbparams)
        self.cursor = self.conn.cursor()
        self._sql = None

    def process_item(self, item, spider):
        self.cursor.execute(self.sql, (item['title'], item['company'], item['location'], item['job_description']))
        self.conn.commit()
        return item

    @property
    def sql(self):
        if not self._sql:
            self._sql = """
            insert into jobinfo(id,title,company,location,job_description) values (null,%s,%s,%s,%s)
            """
            return self._sql
        return self._sql


class MysqlJobsTwistedPipeline(object):
    def __init__(self):
        dbparams = {
            'host': '127.0.0.1',
            'port': 3306,
            'user': 'root',
            'password': 'root',
            'database': 'nlpjob',
            'charset': 'utf8',
            'cursorclass': cursors.DictCursor,
        }
        self.dbpool = adbapi.ConnectionPool('pymysql', **dbparams)
        self._sql = None

    @property
    def sql(self):
        if not self._sql:
            self._sql = """
            insert into jobinfo(id,title,company,location,job_description) values (null,%s,%s,%s,%s)
            """
            return self._sql
        return self._sql

    def process_item(self, item, spider):
        asyn_item = copy.deepcopy(item)
        item_info = {
            'title': asyn_item['title'],
            'company': asyn_item['company'],
            'location': asyn_item['location'],
            'job_description': asyn_item['job_description'],
        }
        defer = self.dbpool.runInteraction(self.insert_item, item_info)
        defer.addErrback(self.handle_error, item_info, spider)
        return item

    def insert_item(self, cursor, item):
        cursor.execute(self.sql, (item['title'], item['company'], item['location'], item['job_description']))

    def handle_error(self, error, item, spider):
        print("MysqlJobsTwistedPipeline====> error: {error}")
