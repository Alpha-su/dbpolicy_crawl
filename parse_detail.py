#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2021/2/4 下午4:56
# @Author  : Alphasu
# @Function: 从Redis获取数据，发起request请求爬取并解析数据

from aiomultiprocess import Pool
import request_tools
import utils
import parse_context
from scrapy import Selector
import asyncio
from utils import remove_js_css
from loguru import logger


def parse_main_text(url, selector2, main_text_pattern):
    img_text, attachment_text = '', ''
    try:
        main_domain = selector2.xpath(main_text_pattern)
        main_text_list = [item.strip() for item in main_domain.xpath('.//text()').extract()]
        main_text = ''.join(main_text_list)
    except Exception:
        main_text = ''
    else:
        if len(main_text) < 100:
            img_list = utils.find_img(url, main_domain)
            img_text = ','.join(img_list)
            attachment_list = utils.find_attachment(url, main_domain)
            attachment_text = ','.join(attachment_list)
            if len(img_text) > 60000:
                img_text = ''
            if len(attachment_text) > 60000:
                attachment_text = ''
    return main_text, attachment_text, img_text


def parse_struct_info(selector0, date_pattern, source_pattern, title_pattern):
    remove_list = ['稿源', '来源', '发布机构', '发布日期', '发文机关']
    if date_pattern:
        try:
            date_raw = selector0.xpath(date_pattern).extract_first()
        except Exception:
            date = ''
        else:
            if isinstance(date_raw, str):
                date = utils.search_date_time(date_raw)
            else:
                date = ''
    else:
        date = ''
    if source_pattern and source_pattern[0] == '/':
        source_pattern = source_pattern.replace('tbody', '')
        if 'text()' not in source_pattern:
            source_pattern = source_pattern + '//text()'
        try:
            source = utils.get_chinese(''.join(selector0.xpath(source_pattern).extract()))
        except Exception:
            source = ''
        else:
            for item in remove_list:
                source = source.replace(item, '')
    else:
        source = source_pattern
    if title_pattern:
        title_pattern = title_pattern.replace('tbody', '')
        if 'text()' not in title_pattern:
            title_pattern = title_pattern + '//text()'
        try:
            title = ''.join(selector0.xpath(title_pattern).extract()).strip()
        except Exception:
            title = ''
        else:
            if title[:2] == "名称" or title[:2] == "标题":
                title = title[3:]
    else:
        title = ''
    return date, source, title


def update_db(db, id_, column, value):
    sql = "update api_links set {}='{}' where id='{}'".format(column, value, id_)
    db.exec_sql(sql)
    # 这里要为之后预留rank变更的接口


def parse(text, db, sub_url, main_text_pattern, date_pattern, source_pattern, title_pattern, id_, title, date_):
    title_in_page, new_date = title, date_
    request_text = remove_js_css(text)
    selector = Selector(text=request_text)
    if date_pattern or source_pattern or title_pattern:
        new_date, source, title_in_page = parse_struct_info(selector, date_pattern, source_pattern, title_pattern)
        if new_date:
            update_db(db, id_, 'pub_date', new_date)
        else:
            new_date = date_
        if source:
            update_db(db, id_, 'source', source)
        if title_in_page and len(utils.get_chinese(title_in_page)) > len(utils.get_chinese(title)) and title_in_page[-3:] != '...':
            update_db(db, id_, 'title', title_in_page)
        else:
            title_in_page = utils.fulfill_title(title, selector)
            if title_in_page[-3:] != '...':
                update_db(db, id_, 'title', title_in_page)
    else:
        if title[-3:] == '...':
            title_in_page = utils.fulfill_title(title, selector)
            if title_in_page[-3:] != '...':
                update_db(db, id_, 'title', title_in_page)
    if main_text_pattern:
        main_text, attachment, img = parse_main_text(sub_url, selector, main_text_pattern)
    else:
        try:
            task = parse_context.MAIN_TEXT(sub_url, request_text)
            result_dict = task.main()
            main_text = result_dict['content']
            img = ','.join(result_dict['img'])
            attachment = ','.join(result_dict['attachment'])
        except Exception:
            main_text, img, attachment = '', '', ''
    return main_text, attachment, img, title_in_page, new_date


def save_data(db, data_dict):
    insert_dict = {
        'main_text': data_dict['main_text'],
        'img': data_dict['img'],
        'attachment': data_dict['attachment'],
        'rank': data_dict['rank'],
        'links_id': data_dict['links_id']
    }
    db.insert_one('api_details', insert_dict)


async def handle_sub_url(task_dict, db):
    result_dict = {
        'title': task_dict['title'],
        'date': task_dict['date'],
        'main_text': '',
        'img': '',
        'attachment': '',
        'rank': 0,
        'links_id': task_dict['link_id']
    }
    if task_dict['sub_url'][-4:] in {'.pdf', '.doc', '.docx', '.txt'}:
        result_dict['attachment'] = task_dict['sub_url']
        rank = utils.get_rank(result_dict)
    else:
        request = request_tools.Request(url=task_dict['sub_url'])
        await request.get_page_async()
        content = request.text
        # print(task_dict['sub_url'], request.status_info)
        if content:  # 连接成功
            result_dict['main_text'], result_dict['attachment'], result_dict['img'], result_dict['title'], result_dict['date'] = \
                parse(content, db, task_dict['sub_url'], task_dict['main_text_pattern'], task_dict['date_pattern'],
                      task_dict['source_pattern'], task_dict['title_pattern'], task_dict['link_id'], task_dict['title'], task_dict['date'])
        rank = utils.get_rank(result_dict)
        if request.status_info == '200':
            logger.success(f"访问{task_dict['sub_url']}成功, 文本完整性等级为{rank}")
        else:
            logger.warning(f"访问{task_dict['sub_url']}失败, 错误代码{request.status_info}")
    result_dict['rank'] = rank
    save_data(db, result_dict)


async def manage(task_id):
    logger.info("task %s is running" % task_id)
    db_web = utils.init_mysql()
    db_r = utils.init_redis()
    while True:
        try:
            # lpop 获取队列最左边的数据，并且从队列删除这个数据，所以，这个任务可以避免被多台服务器都去执行
            task_info = db_r.spop('links')
        except Exception as e:
            logger.exception(e)
            await asyncio.sleep(30)
            db_r = utils.init_redis()
            continue
        # 如果 队列中没有任务数据，此时在 python 中返回的是 None
        if task_info:
            # 将字符串转换成字典，也就是解析任务
            task_dict = db_web.select('api_links', 'id,title,pub_date,sub_url,config_id', condition="sub_url='{}'".format(task_info), fetch_one=True)
            if not task_dict:
                logger.error(f"任务{task_info}不存在")
                await asyncio.sleep(300)
                continue
            config_dict = db_web.select('api_config', 'main_text_pattern, date_pattern, source_pattern, title_pattern', 'id=%s' % str(task_dict['config_id']), fetch_one=True)
            task_dict['link_id'] = task_dict['id']
            task_dict['date'] = task_dict['pub_date'].strftime('%Y-%m-%d') if task_dict['pub_date'] else ''
            task_dict['main_text_pattern'] = config_dict['main_text_pattern']
            task_dict['date_pattern'] = config_dict['date_pattern']
            task_dict['source_pattern'] = config_dict['source_pattern']
            task_dict['title_pattern'] = config_dict['title_pattern']
            task_dict.pop('id')
            task_dict.pop('config_id')
            task_dict.pop('pub_date')
            # print(task_dict)
            await handle_sub_url(task_dict, db_web)
            # db_r.sadd('link_id', task_info)
        else:
            await asyncio.sleep(300)


async def main():
    async with Pool() as pool:
        await pool.map(manage, [i for i in range(16)])


def check_redis():
    db_r = utils.init_redis()
    print(db_r.scard('links'))


def fulfill_task_queue():
    db = utils.init_mysql()
    db_r = utils.init_redis()
    tasks = db.select('api_links', 'sub_url',
                      "id not in (select links_id from api_details)",
                      fetch_one=False)
    for i, task_dict in enumerate(tasks):
        if i % 1000 == 0:
            print(i)
        db_r.sadd('links', str(task_dict['sub_url']))


if __name__ == '__main__':
    # manage()
    # check_redis()
    # asyncio.run(main())
    # utils.wait_redis(db_r)
    asyncio.run(main())
