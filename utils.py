#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2021/2/4 下午4:56
# @Author  : Alphasu
# @Function: 所有的工具代码
import asyncio
import math
import random
import re
import time
from datetime import date
from urllib.parse import urljoin
import redis
from pyppeteer import launch
from pyppeteer.errors import NetworkError
import database
from config import CRAWL_SPEED, BROWER, MYSQL, REDIS


async def init_browser():
    # 启动浏览器
    browser = await launch(BROWER)
    return browser


def load_config_file(db):
    # 为了便于断点重启而进行的从数据库中获取还没运行的文件
    pattern_list = db.select('api_config')
    tmp = db.select('api_status', target='config_id', fetch_one=False)
    if tmp:
        already_solved = set(item['config_id'] for item in tmp)
    else:
        already_solved = set()
    ret_list = [dict_ for dict_ in pattern_list if dict_['id'] not in already_solved]
    return ret_list


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


def test_request(sub_links):
    # 检查是否必须使用浏览器爬取子链接
    # count_for_cant_request = 0  # 用来计数，只有出现所有子链接都无法识别的情况，才使用浏览器
    # door = max(int(len(sub_links) / 2), 3)  # 退出的阈值
    for title in list(sub_links.keys()):
        if 'http' not in sub_links[title][0]:
            return True
        # if (count_for_cant_request >= door) or ('http' not in sub_links[title][0]):
        #     self.use_browser = True
        #     error_info = u'2使用浏览器捕获数据'
        #     self.error_info.append(error_info)
        #     return
        # if sub_links[title][0][-4:] in {'.pdf', '.doc', '.docx', '.xls', '.xlsx', '.txt', '.csv'}:  # 子链接即为pdf附件的
        #     continue
        # request = Request(sub_links[title][0])
        # request.get_page()
        # if not request.text:  # 无法连接
        #     count_for_cant_request += 1
        #     continue
        # else:
        #     if self.main_text_pattern:
        #         main_text, attachment, img = self.parse_main_text(sub_links[title][0], request.text)
        #         if len(main_text) > 30:
        #             self.use_browser = False
        #             return
        #         elif any([main_text, attachment, img]):
        #             # 最起码要有一个捕获到数据算是认为页面请求有效
        #             continue
        #         else:
        #             count_for_cant_request += 1
        #     else:
        #         task = parse_context.MAIN_TEXT(url=sub_links[title][0], text=request.text)
        #         try:
        #             result_dict = task.main()
        #         except Exception:
        #             count_for_cant_request += 1
        #         else:
        #             if int(result_dict['state']) == 1:
        #                 # 　为1表示正常提取
        #                 self.use_browser = False
        #                 return  # 但凡出现能够完全正常识别的情况，直接返回
        #             elif result_dict['attachment']:
        #                 continue
        #             else:
        #                 count_for_cant_request += 1


async def request_check(req):
    # 请求过滤
    if req.resourceType in ['image', 'media']:
        await req.abort()
    else:
        await req.continue_()


async def handle_dialog(dialog):
    await dialog.dismiss()


def wait_redis(db_redis, max_length=CRAWL_SPEED['Redis_Stack'], sleep_gap=CRAWL_SPEED['Redis_Delay']):
    while True:
        len_ = db_redis.scard('links')
        if len_ >= max_length:
            time.sleep(sleep_gap)
            print("redis stack is full!", len_)
        else:
            break


def find_xpath_case_in_frames(frame_list, index):
    if not frame_list:
        return 0
    else:
        for tmp_frame in [frame_list[index]] + frame_list:
            case = find_xpath_case(tmp_frame.selector)
            if case != 0:
                return case
    return 0


def get_rank(data_dict):
    # 用于判定文件完整性级别
    if data_dict['main_text'] and len(data_dict['main_text']) > 20:
        rank = 1
    elif data_dict['attachment']:
        rank = 2
    elif data_dict['img']:
        rank = 3
    else:
        rank = 4
    if not data_dict['date']:
        rank += 8
    if data_dict['title'][-3:] == '...':
        rank += 16
    return rank


def fulfill_title(title, selector):
    # 自动化判断补全title
    tmp_title = title[:-4]
    be_solved_title = []  # 带筛选的标题
    for tag in selector.xpath('//body//*'):
        text = ''.join(tag.xpath('.//text()').extract()).strip()
        if text.startswith(tmp_title):
            be_solved_title.append(text)
    if be_solved_title:
        return min(be_solved_title, key=len)  # 注意min函数key的用法
    else:
        return title


def init_redis(machine_name="dbpolicy"):
    pool = redis.ConnectionPool(host=REDIS[machine_name], port=6379, decode_responses=True, password=REDIS["password"])
    db_r = redis.Redis(connection_pool=pool)
    return db_r


def init_mysql(machine_name="dbpolicy", use_flow=False):
    return database.Mysql(MYSQL["user"], MYSQL["password"], MYSQL["database"], host=MYSQL[machine_name], use_flow=use_flow)


def remove_js_css(content):
    #  (<script>....</script> and <style>....</style> <!-- xxx -->)
    r = re.compile(r'<script.*?</script>', re.I | re.M | re.S)
    s = r.sub('', content)
    r = re.compile(r'<style.*?</style>', re.I | re.M | re.S)
    s = r.sub('', s)
    r = re.compile(r'<link.*?>', re.I | re.M | re.S)
    s = r.sub('', s)
    r = re.compile(r'<meta.*?>', re.I | re.M | re.S)
    s = r.sub('', s)
    r = re.compile(r'<ins.*?</ins>', re.I | re.M | re.S)
    s = r.sub('', s)
    return s


def get_chinese(attribute_name):
    # 只提取里面的中文
    if not attribute_name:
        return ''
    line = re.sub('[^\u4e00-\u9fa5]', '', str(attribute_name))
    return line


def search_date_time(string):
    string = re.sub('[ \t\n]', '', string)  # 避免因网页格式而造成的失效
    date_form = [
        "(\d{4})[-|/.](\d{1,2})[-|/.](\d{1,2})",
        "(\d{2})[-|/.](\d{1,2})[-|/.](\d{1,2})",
        "(\d{4})年(\d{1,2})月(\d{1,2})日",
        "(\d{2})年(\d{1,2})月(\d{1,2})日",
    ]
    for form in date_form:
        result = re.search(form, string, re.S)
        if result:
            year = result.group(1)
            if len(year) < 4:
                if int(year) < 60:
                    year = '20' + year
                else:
                    year = '19' + year
            month = result.group(2)
            if len(month) == 1:
                month = '0' + month
            day = result.group(3)
            if len(day) == 1:
                day = '0' + day
            try:
                date_obj = date(int(year), int(month), int(day))
            except Exception:
                return None
            else:
                if date_obj > date.today():
                    return None
                else:
                    return year + '-' + month + '-' + day
    return None


def find_attachment(url, domain):
    attachment_list = []
    a_list = domain.xpath('.//a[@href != "" and text() != ""]')
    for a in a_list:
        a_text = ''.join(a.xpath('.//text()').extract())
        a_href = a.xpath('.//@href').extract_first().strip()
        pattern1 = re.compile("(.doc|\.docx|\.pdf|\.csv|\.xlsx|\.xls|\.txt)")  # 找到文件后缀
        result1 = pattern1.findall(a_text)
        pattern2 = re.compile('附件')  # 找到附件字样
        result2 = pattern2.findall(a_text)
        if result1 or result2:
            attachment_list.append(str(a_text) + '(' + str(urljoin(url, a_href)) + ')')
    return attachment_list


def find_img(url, domain):
    image_list = []
    img_li = domain.xpath('.//img[@src != ""]')
    for img in img_li:
        img_src = img.xpath('.//@src').extract_first()
        image_list.append(urljoin(url, img_src))
    return image_list


def find_max_text(node):
    # 从节点中选取最长字段（包括title属性）作为结果返回
    max_text = node.xpath('./@title').extract_first()
    if not max_text:
        max_len = 0
    else:
        max_len = len(get_chinese(max_text))
    for te in node.xpath('.//text()').extract():
        len_te = len(get_chinese(te))
        if len_te > max_len:
            max_text = te
            max_len = len_te
    return max_text, max_len


def find_xpath_case(selector):
    if selector.xpath('/html/body//*[text()="下一页" or text()="后一页" or text()="下页" or text()="后页" or text() ="后一页>>"]'):
        # 可以按页码翻页，按页码翻页比按下一页翻页更稳定准确
        xpath_case = 1
    elif selector.xpath('/html/body//*[@title="下一页" or @title="后一页" or @title="下页"]'):
        xpath_case = 4
    elif selector.xpath('/html/body//a[text()=2]'):
        xpath_case = 2
    elif selector.xpath('/html/body//*[text()="[2]"]'):
        xpath_case = 6
    elif selector.xpath('/html/body//*[text()=2]'):
        xpath_case = 3
    elif selector.xpath('/html/body//*[text()="下一页>>" or text()="[下一页]"]'):
        xpath_case = 5
    elif selector.xpath('/html/body//*[text()=">>"]'):
        xpath_case = 7
    else:
        xpath_case = 0
    return xpath_case


def from_xpath_case_to_xpath(xpath_case, page_num):
    if xpath_case == 2:
        return '/html/body//a[text() = {page_num}]'.format(page_num=str(page_num + 1))
    elif xpath_case == 1:
        return '/html/body//*[text()=\\"下一页\\" or text()=\\"后一页\\" or text()=\\"下页\\" or text()=\\"后页\\" or text(' \
               ')=\\"后一页>>\\"] '
    elif xpath_case == 3:
        return '/html/body//*[text()={page_num}]'.format(page_num=str(page_num + 1))
    elif xpath_case == 4:
        return '/html/body//*[@title=\\"下一页\\" or @title = \\"后一页\\" or @title=\\"下页\\"]'
    elif xpath_case == 5:
        return '/html/body//*[text()=\\"下一页>>\\" or text()=\\"[下一页]\\"]'
    elif xpath_case == 6:
        return '/html/body//*[text() = \\"[{page_num}]\\"]'.format(page_num=str(page_num + 1))
    elif xpath_case == 7:
        return '/html/body//*[text()=\\">>\\"]'
    else:
        return None


async def get_content(frame, retries=0):
    if retries > 6:
        return await frame.content()
    else:
        try:
            return await frame.content()
        except NetworkError:
            await asyncio.sleep(1)
            return await get_content(frame, retries + 1)


def is_words_in_string(words_set, string):
    """
    判断某个字符串中是否包含某个字符集
    :param words_set: 字符集
    :param string: 字符串
    :return: True or False
    """
    if not (words_set and string):
        return False
    else:
        for word in words_set:
            if word in string:
                return True
    return False


def record_times(times, limit=1000):
    """
    在循环中，根据一定的循环次数进行报数
    :param times: 循环次数
    :param limit: 报数的阈值，默认每循环1000次进行报数
    :return: None
    """
    if times % limit == 0:
        print("循环进行到第 {} 次".format(str(times)))
