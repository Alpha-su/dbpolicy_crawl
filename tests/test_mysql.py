#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2021/2/10 下午5:10
# @Author  : Alphasu
# @Features: mysql连接池操作代码测试用例
import pytest
from database import Mysql
import os
from random import random
import yaml

config_test = {
    'host': '127.0.0.1',
    'port': 3306,
    'user': 'alphasu',
    'password': '990211',
    'db': 'test',
    'charset': 'utf8',
}


class Testmysql:
    def setup_class(self):
        # 加载数据库配置文件
        with open('/home/alphasu/PycharmProjects/dbpolicy_crawl/config/database.yml', 'r') as f:
            config = yaml.safe_load(f.read())
        self.config = config['mysql'].get('test')

    def test_create_conn(self):
        db = Mysql.get_db(self.config)
        conn1 = db.get_conn()
        conn2 = db.get_conn()
        assert db.conn_size == 2
        conn3 = db.get_conn()
        assert db.conn_size == 4
        db.pool.put(conn1)
        db.pool.put(conn2)
        db.pool.put(conn3)

    def test_insert_one(self):
        insert_data = {
            "a": int(random() * 10),
            "b": int(random() * 10),
            "c": int(random() * 10),
        }
        assert Mysql.get_db(self.config).insert_one('test_table1', insert_data) is True

    def test_select(self):
        assert len(Mysql.get_db(self.config).select("test_table1")) > 0

    def test_insert_many(self):
        ins_lst = []
        for i in range(3):
            ins_lst.append({
                "a": int(random() * 10),
                "b": int(random() * 10),
                "c": int(random() * 10),
            })
        assert Mysql.get_db(self.config).insert_many("test_table1", ins_lst) is True

    def test_delete(self):
        assert Mysql.get_db(self.config).delete('test_table1', 'id=1')



