# -*- coding: utf-8 -*-
from pprint import pprint

import pymysql
# import pymongo


class Mysql:
    def __init__(self, user, password, db, port=3306, charset='utf8', host='localhost', use_flow=False):
        try:
            self.db = pymysql.connect(host=host, user=user, password=password,
                                      port=port, charset=charset, db=db)
            if use_flow:
                # 流式数据读取
                self.cursor = self.db.cursor(cursor=pymysql.cursors.SSDictCursor)
            else:
                self.cursor = self.db.cursor(cursor=pymysql.cursors.DictCursor)  # 字典的游标读取
                # self.cursor = self.db.cursor()
        except pymysql.Error as e:
            print('连接数据库失败' + '  ' + str(e))
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        # 提交数据库并执行
        try:
            self.db.commit()
            # 关闭游标
            self.cursor.close()
            # 关闭数据库连接
            self.db.close()
        except pymysql.Error as e:
            print('关闭数据库失败' + '  ' + str(e))
    
    def insert_one(self, table, data):
        """
        table: 插入的数据表名
        data: 字典型数据
        """
        keys = ','.join(data.keys())
        value = ','.join(['%s'] * len(data))
        sql = 'INSERT INTO {table}({keys}) VALUES ({value})'.format(table=table, keys=keys, value=value)
        try:
            if self.cursor.execute(sql, tuple(data.values())):
                self.db.commit()
                return True
        except Exception as err:
            # print(err)
            err_code = int(str(err)[1:5])
            print('error_code:　' + str(err_code))
            if err_code != 1062:
                # print(data)
                print(err)
                print(data)
            self.db.rollback()
            return False
    
    def insert_many(self, table, raw_data_list):
        """
        table: 插入的数据表名字
        data_list: 由字典组成的列表
        """
        keys = ','.join(raw_data_list[0].keys())
        if (length := len(raw_data_list)) > 1000:
            # 这里要注意：不能一次插入太多数据，既有效率的考虑，也有内存的考虑，一般以一次插入1000条为宜
            ins_data_list = [raw_data_list[i:i + 1000] for i in range(0, length, 1000)]
        else:
            ins_data_list = [raw_data_list]
        for data_list in ins_data_list:
            value = ','.join(['%s'] * len(data_list[0]))
            sql = 'INSERT IGNORE INTO {table}({keys}) VALUES ({value})'.format(table=table, keys=keys, value=value)
            try:
                if self.cursor.executemany(sql, tuple(tuple(data.values()) for data in data_list)):
                    self.db.commit()
            except Exception as e:
                print(e)
                self.db.rollback()
            
    def select(self, table, target='*', condition='true', fetch_one=False):
        """
        获取所有结果
        """
        sql = 'SELECT {target} FROM {table} WHERE {condition}'.format(target=target, table=table, condition=condition)
        try:
            self.cursor.execute(sql)
            if fetch_one:
                return self.cursor.fetchone()
            else:
                return self.cursor.fetchall()
        except Exception as e:
            print(e)
        
    def update(self, sql):
        try:
            self.cursor.execute(sql)
            self.db.commit()
        except Exception as e:
            self.db.rollback()
            print(e)
            return False
        else:
            return True
    
    def delete(self, table, condition):
        sql = 'DELETE FROM {table} WHERE {condition}'.format(table=table,condition=condition)
        try:
            self.cursor.execute(sql)
            self.db.commit()
        except Exception as e:
            print(e)
            self.db.rollback()
            return False
        else:
            return True


# class Mongodb():
#     def __init__(self, mongo_db, mongo_collection, host = 'localhost',port = 27017):
#         self.client = pymongo.MongoClient(host=host,port=port)
#         self.db = self.client[mongo_db]
#         self.collection = mongo_collection
#
#     def save_to_mongo(self,result):
#         try:
#             self.db[self.collection].insert(result)
#         except Exception:
#             print('存储到MongoDB失败')




