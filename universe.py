# -*- coding: utf-8 -*-
import time
from aiomultiprocess import Pool
import asyncio
import math
import csv
from pyppeteer import launch
import database
import random
from master import Parser
from config import CRAWL_SPEED


async def init_browser():
    # 启动浏览器
    browser = await launch({
        # 'headless': False,  # 关闭无头模式
        'args': [
            '--log-level=3  ',
            '--disable-images',
            '--disable-extensions',
            '--hide-scrollbars',
            '--disable-bundled-ppapi-flash',
            '--mute-audio',
            '--no-sandbox',
            '--disable-setuid-sandbox',
            '--disable-gpu',
            '–single-process',  # 将Dom的解析和渲染放到一个进程，省去进程间切换的时间
            '--disable-infobars',  # 禁止信息提示栏
            '--disable-dev-shm-usage',  # 禁止使用/dev/shm，防止内存不够用,only for linux
            '--no-default-browser-check',  # 不检查默认浏览器
            '--disable-hang-monitor',  # 禁止页面无响应提示
            '--disable-translate',  # 禁止翻译
            '--disable-setuid-sandbox',
            '--no-first-run',
            '--no-zygote',
        ],
        'dumpio': True,
        'LogLevel': 'WARNING',
    })
    return browser


def load_config_file(db):
    pattern_list = db.select('api_config')
    tmp = db.select('api_status', target='config_id', fetch_one=False)
    if tmp:
        already_solved = set(item['config_id'] for item in tmp)
    else:
        already_solved = set()
    ret_list = [dict_ for dict_ in pattern_list if dict_['id'] not in already_solved]
    return ret_list


async def page_solver(config_list):
    db = database.Mysql('root', '990211', 'dbpolicy_web', host='121.36.22.40')
    browser = await init_browser()
    for config in config_list:
        print('begin to process:{}'.format(config['target_url']))
        parser = Parser(config, browser, db=db, mode=CRAWL_SPEED['MODE'])
        db.insert_one('api_status', {'status': 2, 'pages': 0, 'counts': 0, 'error_info': '', 'config_id': config['id']})
        try:
            state, page_num = await parser.manager()
        except Exception as e:
            error_info = u'1整个解析过程存在问题 ' + str(e)
            sql = 'update api_status set status=4, pages="{}", counts="{}", error_info="{}" where config_id="{}"'.format(0, 0, error_info, config['id'])
        else:
            error_info = ','.join(parser.error_info)
            if state:
                sql = 'update api_status set status=3, pages="{}", counts="{}", error_info="{}" ' \
                      'where config_id="{}"'.format(page_num, parser.file_count, error_info, config['id'])
            else:
                sql = 'update api_status set status=4, pages="{}", counts="{}", error_info="{}" ' \
                      'where config_id="{}"'.format(page_num, parser.file_count, error_info, config['id'])
        db.update(sql)
    await browser.close()


def split_task(item_list):
    # 任务划分
    random.shuffle(item_list)
    return_list = []
    try:
        step = math.ceil(len(item_list) / CRAWL_SPEED['Chromium_Num'])
        if step == 0:
            return return_list
    except Exception:
        return return_list
    i = 0
    tmp_list = []
    for dli in item_list:
        i += 1
        tmp_list.append(dli)
        if i == step:
            return_list.append(tmp_list)
            tmp_list = []
            i = 0
    if tmp_list:
        return_list.append(tmp_list)
    return return_list


async def main():
    mysql_db = database.Mysql('root', '990211', 'dbpolicy_web', host='121.36.22.40')
    config_dli = load_config_file(mysql_db)
    config_list = split_task(config_dli)
    async with Pool() as pool:
        await pool.map(page_solver, config_list)


async def debug():
    print('debug!')
    db = database.Mysql('root', '990211', 'dbpolicy_web', host='121.36.22.40')
    config_id = 514
    config = db.select('api_config', condition='id="{}"'.format(config_id), fetch_one=True)
    print(config)
    browser = await init_browser()
    parser = Parser(config, browser, db=db, mode=CRAWL_SPEED['MODE'])
    try:
        state, i = await parser.manager()
        print(state, i)
        print(parser.error_info)
    except Exception as e:
        print("整个解析过程异常终止" + str(e))
    await browser.close()


def begin():  # 工厂函数
    if CRAWL_SPEED['MODE'] == 'debug':
        asyncio.run(debug())
    else:
        asyncio.run(main())


if __name__ == '__main__':
    begin()
