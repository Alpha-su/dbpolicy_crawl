#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2021/2/4 下午4:56
# @Author  : Alphasu
# @Function: 爬虫程序启动文件，主要实现任务分发和多进程、协程爬取
from loguru import logger
from datetime import datetime
from aiomultiprocess import Pool
import asyncio
from scheduler import Parser
from config import CRAWL_SPEED
from utils import init_browser, load_config_file, split_task, init_mysql


async def page_solver(config_list):
    db = init_mysql()
    browser = await init_browser()
    for config in config_list:
        logger.info('begin to process:{}'.format(config['target_url']))
        parser = Parser(config, browser, db=db, mode=CRAWL_SPEED['MODE'])
        try:
            state, page_num = await parser.manager()
        except Exception as e:
            logger.error(f"{config['target_url']}整个解析过程异常终止" + str(e))
        else:
            error_info = ','.join(parser.error_info)
            if state:
                logger.success(f"{config['target_url']}解析完成，共解析{page_num}页")
            else:
                logger.warning(f"{config['target_url']}解析中存在异常: {error_info}")
        db.insert_one('api_status', {'status': 2, 'pages': 0, 'counts': 0, 'error_info': '', 'config_id': config['id']})
    await browser.close()


async def main():
    mysql_db = init_mysql()
    config_dli = load_config_file(mysql_db, CRAWL_SPEED['MODE'])
    config_list = split_task(config_dli)
    async with Pool() as pool:
        await pool.map(page_solver, config_list)


async def debug():
    logger.info("Begin to run in debug mode")
    db = init_mysql()
    config_id = 1711
    config = db.select('api_config', condition='id="{}"'.format(config_id), fetch_one=True)
    logger.debug(config)
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
