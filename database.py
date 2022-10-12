#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2021/2/4 下午4:56
# @Author  : Alphasu
# @Function: 数据库操作工具包
import pymysql
from queue import Queue, Empty
import os
import logging


class Mysql:
    __v = None
    config_dbpolicy = {
        'host': '121.36.22.40',
        'port': 3306,
        'user': 'root',
        'password': os.environ.get("MYSQL_PWD", ""),
        'db': 'dbpolicy_web',
        'charset': 'utf8',
    }

    def __init__(self, config=None, max_conn=32):
        """
        初始化创建Mysql连接池
        :param config: Mysql连接配置
        :param max_conn: 最大连接数
        """
        if config is None:
            self.config = self.config_dbpolicy
        else:
            self.config = config
        self.max_conn = max_conn
        self.conn_size = 1
        self.pool = Queue(self.max_conn)
        self.create_conn()

    def __del__(self):
        for i in range(self.conn_size):
            self.pool.get().close()

    @classmethod
    def get_db(cls, *args, **kwargs):
        """
        单例模式设计模式工厂函数
        :param args: 构造函数参数
        :param kwargs: 构造函数参数
        :return: 类的唯一实例
        """
        if cls.__v:
            return cls.__v
        else:
            cls.__v = Mysql(*args, **kwargs)
            return cls.__v

    def create_conn(self):
        for i in range(self.conn_size):
            try:
                conn = pymysql.connect(**self.config)
                conn.autocommit(True)
                self.pool.put(conn)
            except Exception as e:
                raise IOError(e)

    def get_conn(self):
        try:
            return self.pool.get(timeout=self.conn_size)
        except Empty:
            if self.conn_size < self.max_conn:
                self.create_conn()
                self.conn_size *= 2
            return self.pool.get()

    def insert_one(self, table, data):
        """
        插入一条数据
        :param table: 插入的数据表名
        :param data: 插入数据 dict
        :return: True or False
        """
        if not isinstance(data, dict):
            raise TypeError("Data type must be dict, not others")
        conn = self.get_conn()
        cursor = conn.cursor(cursor=pymysql.cursors.DictCursor)
        keys = ','.join(data.keys())
        value = ','.join(['%s'] * len(data))
        sql = 'INSERT INTO {table}({keys}) VALUES ({value})'.format(table=table, keys=keys, value=value)
        try:
            cursor.execute(sql, tuple(data.values()))
            return True
        except Exception as e:
            print("Mysql error in insert_one: " + str(e))
            return False
        finally:
            cursor.close()
            self.pool.put(conn)
    
    def insert_many(self, table, raw_data_list):
        """
        批量插入
        :param table: 插入的数据表名字
        :param raw_data_list: 由字典组成的列表
        :return:
        """
        conn = self.get_conn()
        cursor = conn.cursor(cursor=pymysql.cursors.DictCursor)
        keys = ','.join(raw_data_list[0].keys())
        value = ','.join(['%s'] * len(raw_data_list[0]))
        sql = 'INSERT IGNORE INTO {table}({keys}) VALUES ({value})'.format(table=table, keys=keys, value=value)
        try:
            cursor.executemany(sql, tuple(tuple(data.values()) for data in raw_data_list))
            return True
        except Exception as e:
            print("Mysql error in insert_many: " + str(e))
            return False
        finally:
            cursor.close()
            self.pool.put(conn)
            
    def select(self, table, target='*', condition='true', fetch_one=False, use_flow=False):
        """
        通过查询语句获取结果
        :param table: 查询的数据表
        :param target: 查询的字段名，默认为全部字段
        :param condition: 查询的条件语句
        :param fetch_one: 是否只返回一条数据
        :param use_flow: 是否使用流式数据处理
        :return: 返回的结果
        """
        conn = self.get_conn()
        if use_flow:
            cursor = conn.cursor(cursor=pymysql.cursors.SSDictCursor)
        else:
            cursor = conn.cursor(cursor=pymysql.cursors.DictCursor)
        sql = 'SELECT {target} FROM {table} WHERE {condition}'.format(target=target, table=table, condition=condition)
        try:
            cursor.execute(sql)
            if fetch_one:
                return cursor.fetchone()
            else:
                return cursor.fetchall()
        except Exception as e:
            print("Mysql error in select: " + str(e))
            return None
        finally:
            cursor.close()
            self.pool.put(conn)

    def exec_sql(self, sql):
        """
        执行sql语句
        :param sql: sql语句
        :return: 执行结果
        """
        conn = self.get_conn()
        cursor = conn.cursor(cursor=pymysql.cursors.DictCursor)
        try:
            cursor.execute(sql)
            return True
        except Exception as e:
            print("Mysql error in sql: {}, error is: {}".format(str(sql), str(e)))
            return False
        finally:
            cursor.close()
            self.pool.put(conn)

    def delete(self, table, condition):
        sql = 'DELETE FROM {table} WHERE {condition}'.format(table=table, condition=condition)
        return self.exec_sql(sql)


if __name__ == '__main__':
    from utils import init_mysql
    db = init_mysql()
    rst = db.select('api_links', target='title, sub_url', condition='loc_id="{}"'.format(153))
    print(rst)
