from urllib.parse import urljoin
from aiomultiprocess import Pool
from database import Mysql
import redis
import R2
import re
import parse_context
from scrapy import Selector
import asyncio
from datetime import date


def get_chinese(attribute_name):
    # 只提取里面的中文
    if not attribute_name:
        return ''
    line = re.sub('[^\u4e00-\u9fa5]', '', str(attribute_name))
    return line


def remove_js_css(content):
    # remove the the javascript and the stylesheet and the comment content
    # (<script>....</script> and <style>....</style> <!-- xxx -->)
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


def find_img(url, domain):
    image_list = []
    img_li = domain.xpath('.//img[@src != ""]')
    for img in img_li:
        img_src = img.xpath('.//@src').extract_first()
        image_list.append(urljoin(url, img_src))
    return image_list


def find_attachment(url, domain):
    attachment_list = []
    a_list = domain.xpath('.//a[@href != "" and text() != ""]')
    for a in a_list:
        a_text = ''.join(a.xpath('.//text()').extract())
        a_href = a.xpath('.//@href').extract_first().strip()
        pattern1 = re.compile('(.doc|\.docx|\.pdf|\.csv|\.xlsx|\.xls|\.txt)')  # 找到文件后缀
        result1 = pattern1.findall(a_text)
        pattern2 = re.compile('附件')  # 找到附件字样
        result2 = pattern2.findall(a_text)
        if result1 or result2:
            attachment_list.append(str(a_text) + '(' + str(urljoin(url, a_href)) + ')')
    return attachment_list


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
            img_list = find_img(url, main_domain)
            img_text = ','.join(img_list)
            attachment_list = find_attachment(url, main_domain)
            attachment_text = ','.join(attachment_list)
            if len(img_text) > 60000:
                img_text = ''
            if len(attachment_text) > 60000: attachment_text = ''
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
                date = search_date_time(date_raw)
            else:
                date = ''
    else:
        date = ''
    if source_pattern and source_pattern[0] == '/':
        source_pattern = source_pattern.replace('tbody', '')
        if 'text()' not in source_pattern:
            source_pattern = source_pattern + '//text()'
        try:
            source = get_chinese(''.join(selector0.xpath(source_pattern).extract()))
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


def update_db(db, id, column, value):
    sql = 'update api_links set {}="{}" where id={}'.format(column, value, id)
    db.update(sql)
    # 这里要为之后预留rank变更的接口


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


def parse(text, db, sub_url, main_text_pattern, date_pattern, source_pattern, title_pattern, id, title, date_):
    title_in_page, new_date = title, date_
    request_text = remove_js_css(text)
    selector = Selector(text=request_text)
    if date_pattern or source_pattern or title_pattern:
        new_date, source, title_in_page = parse_struct_info(selector, date_pattern, source_pattern, title_pattern)
        if new_date:
            update_db(db, id, 'pub_date', new_date)
        else:
            new_date = date_
        if source:
            update_db(db, id, 'source', source)
        if title_in_page and len(get_chinese(title_in_page)) > len(get_chinese(title)) and title_in_page[-3:] != '...':
            update_db(db, id, 'title', title_in_page)
        else:
            title_in_page = fulfill_title(title, selector)
            if title_in_page[-3:] != '...':
                update_db(db, id, 'title', title_in_page)
    else:
        if title[-3:] == '...':
            title_in_page = fulfill_title(title, selector)
            if title_in_page[-3:] != '...':
                update_db(db, id, 'title', title_in_page)
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
    else:
        request = R2.Request(url=task_dict['sub_url'])
        await request.get_page_async()
        content = request.text
        print(task_dict['sub_url'], request.status_info)
        if content:  # 连接成功
            result_dict['main_text'], result_dict['attachment'], result_dict['img'], \
            result_dict['title'], result_dict['date'] = parse(content, db,
                                                              task_dict['sub_url'], task_dict['main_text_pattern'],
                                                              task_dict['date_pattern'], task_dict['source_pattern'],
                                                              task_dict['title_pattern'], task_dict['link_id'],
                                                              task_dict['title'], task_dict['date'])
    rank = get_rank(result_dict)
    result_dict['rank'] = rank
    save_data(db, result_dict)


async def manage(task_id):
    print("task %s is running" % task_id)
    db_web = Mysql('root', '990211', 'dbpolicy_web', host='121.36.22.40')
    redis_pool = redis.ConnectionPool(host='121.36.22.40', port=6379, decode_responses=True, password='990211')
    db_r = redis.Redis(connection_pool=redis_pool)
    while True:
        # lpop 获取队列最左边的数据，并且从队列删除这个数据，所以，这个任务可以避免被多台服务器都去执行
        task_info = db_r.spop('links')
        # 如果 队列中没有任务数据，此时在 python 中返回的是 None
        if task_info:
            # 将字符串转换成字典，也就是解析任务
            task_dict = eval(task_info)
            await handle_sub_url(task_dict, db_web)
        else:
            await asyncio.sleep(30)


async def main():
    async with Pool() as pool:
        await pool.map(manage, [i for i in range(16)])


def check_redis():
    pool = redis.ConnectionPool(host='121.36.22.40', port=6379, decode_responses=True, password='990211')
    db_r = redis.Redis(connection_pool=pool)
    print(db_r.scard('links'))


if __name__ == '__main__':
    # manage()
    check_redis()
    # asyncio.run(main())
