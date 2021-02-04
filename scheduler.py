#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2021/2/4 下午5:01
# @Author  : Alphasu
# @Function: 网站爬取任务调度，即主要功能模块
import asyncio
from datetime import datetime
from scrapy import Selector
import parse_context
from frame import Frame
import operator
import copy
from config import CRAWL_SPEED
import utils


class Parser:
    def __init__(self, config, browser, db=None, mode='complete'):
        self.config_id = config['id']
        self.loc_id = config['loc_id']
        self.next_patter = config['next_pattern']
        self.action_pattern = config['action_pattern']
        self.target_url = config['target_url']
        self.item_pattern = config['item_pattern']
        self.main_text_pattern = config['main_text_pattern']
        self.gov = config['gov']
        self.source_pattern = config['source_pattern']
        self.date_pattern = config['date_pattern']
        self.zupei_type = config['zupei_type']
        self.title_pattern = config['title_pattern']
        self.browser = browser
        self.page = None
        self.mode = mode
        self.db = db
        self.max_page = 12000
        self.sub_url_already_crawl = dict()  # 缓存本网站已经进行子链接解析的网页，包括完成了的和未完成的
        self.file_count = 0  # 统计该网页成功解析的子链接数
        self.error_info = list()  # 记录曾出现过的错误信息
        self.sub_url_in_db = self.get_sub_url_in_db()
        self.db_redis = utils.init_redis()
        self.xpath_case = 0  # 翻页规则

    def get_sub_url_in_db(self):
        # 为了避免重复爬取，预加载已经爬取的内容
        ret_set = set()
        tmp_result = self.db.select('api_links', target='title', condition='loc_id="{}"'.format(self.loc_id))
        if tmp_result:
            for item in tmp_result:
                if item:
                    ret_set.add(item['title'])
            return ret_set
        else:
            return ret_set

    async def wait_for_change(self, item_list, max_retries=0):
        retries_times = 0
        while True:
            retries_times += 1
            new_frames = await self.get_frame()
            new_sub_urls = await self.try_to_get_sub_url(new_frames)
            if not operator.eq(new_sub_urls, item_list):
                return
            else:
                if max_retries and retries_times > max_retries:
                    print('休息了很久还是没有翻页成功')
                    return
                else:
                    await asyncio.sleep(1)

    def parse_struct_info(self, selector0):
        remove_list = ['稿源', '来源', '发布机构', '发布日期', '发文机关']
        if self.date_pattern:
            try:
                date_raw = selector0.xpath(self.date_pattern).extract_first()
            except Exception:
                date = ''
            else:
                if isinstance(date_raw, str):
                    date = utils.search_date_time(date_raw)
                else:
                    date = ''
        else:
            date = ''
        if self.source_pattern and self.source_pattern[0] == '/':
            self.source_pattern = self.source_pattern.replace('tbody', '')
            if 'text()' not in self.source_pattern:
                self.source_pattern = self.source_pattern + '//text()'
            try:
                source = utils.get_chinese(''.join(selector0.xpath(self.source_pattern).extract()))
            except Exception:
                source = ''
            else:
                for item in remove_list:
                    source = source.replace(item, '')
        else:
            source = self.source_pattern
        if self.title_pattern:
            self.title_pattern = self.title_pattern.replace('tbody', '')
            if 'text()' not in self.title_pattern:
                self.title_pattern = self.title_pattern + '//text()'
            try:
                title = ''.join(selector0.xpath(self.title_pattern).extract()).strip()
            except Exception:
                title = ''
            else:
                if title[:2] == "名称" or title[:2] == "标题":
                    title = title[3:]
        else:
            title = ''
        return date, source, title

    def parse_main_text(self, url, text):
        main_text, img_text, attachment_text = '', '', ''
        selector = Selector(text=text)
        try:
            main_domain = selector.xpath(self.main_text_pattern)
            main_text_list = [item.strip() for item in main_domain.xpath('.//text()').extract()]
        except Exception:
            task = parse_context.MAIN_TEXT(url=url, text=text)
            try:
                result_dict = task.main()
            except Exception:
                return '', '', ''
            else:
                main_text = result_dict['content']
                img_text = ','.join(result_dict['img'])
                attachment_text = ','.join(result_dict['attachment'])
        else:
            main_text = ''.join(main_text_list)
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

    def save_to_data(self, data_dict):
        max_id = self.db.select('api_links', 'max(id)', fetch_one=True)['max(id)']
        if not max_id:
            new_id = 1
        else:
            new_id = max_id + 1
        ins_to_link = {'id': new_id, 'gov': self.gov, 'title': data_dict['title'], 'pub_date': data_dict['date'],
                       'crawl_date': datetime.now().strftime('%Y-%m-%d %X'), 'sub_url': data_dict['sub_url'], 'zupei_type': self.zupei_type,
                       'source': data_dict['source'], 'loc_id': self.loc_id, 'config_id': self.config_id}
        if self.mode == 'debug':
            print(ins_to_link)
        else:
            res = self.db.insert_one('api_links', ins_to_link)
            if res:
                rank = utils.get_rank(data_dict)
                ins_to_details = {'main_text': data_dict['main_text'], 'attachment': data_dict['attachment'], 'img': data_dict['img'],
                                  'rank': rank,
                                  'links_id': new_id}
                if self.db.insert_one('api_details', ins_to_details):
                    self.file_count += 1

    async def parse_detail(self, sub_links, frame_list, index):
        for title in list(sub_links.keys()):
            self.sub_url_already_crawl[title] = sub_links[title]  # 无论结果如何都要存进去，避免僵死
            sub_url = sub_links[title][0]
            if sub_url[-4:] in {'.pdf', '.doc', '.docx', '.xls', '.xlsx', '.txt', '.csv'}:
                self.save_to_data({'title': title, 'sub_url': sub_url, 'date': sub_links[title][1], 'main_text': '',
                                   'source': '', 'attachment': sub_url, 'img': ''})
                continue
            else:
                xpath = "//a[contains(., '{}') or @title='{}']".format(title, title)
                try:
                    button_selector = frame_list[index].selector.xpath(xpath)
                    button = await frame_list[index].raw_frame.xpath(xpath)
                except Exception:
                    self.save_to_data({'title': title, 'sub_url': sub_url, 'date': sub_links[title][1], 'main_text': '',
                                       'source': '', 'attachment': '', 'img': ''})
                    continue
                else:
                    if not button:
                        self.save_to_data({'title': title, 'sub_url': sub_url, 'date': sub_links[title][1], 'main_text': '',
                                           'source': '', 'attachment': '', 'img': ''})
                        continue
                await button[0].click()
                if button_selector.xpath('./@target').extract_first() == '_blank':
                    await asyncio.sleep(1)
                    while True:
                        pages = await self.browser.pages()
                        if len(pages) == 2:
                            break
                        else:
                            await asyncio.sleep(1)
                    content = await utils.get_content(pages[-1])
                    sub_url = pages[-1].url
                    await pages[-1].close()
                else:
                    await asyncio.sleep(6)
                    pages = await self.browser.pages()
                    if len(pages) == 2:
                        content = await utils.get_content(pages[-1])
                        sub_url = pages[-1].url
                        await pages[-1].close()
                    else:
                        content = await utils.get_content(self.page)
                        sub_url = self.page.url
                        await asyncio.wait(
                            [self.page.goBack(), self.page.waitForXPath(xpath=xpath, timeout=CRAWL_SPEED['CLICK_SUB_URL_MAX_DELAY'])])
                request_text = utils.remove_js_css(content)
                selector = Selector(text=request_text)
                frame_list = await self.get_frame()
            date, source, title_in_page = self.parse_struct_info(selector)
            if not date:  # 为date上双保险
                date = sub_links[title][1]
            if title_in_page:  # 为title上双保险
                save_title = title_in_page
                if (not save_title) or save_title[-3:] == '...' or (len(utils.get_chinese(save_title)) < len(utils.get_chinese(title))):
                    # 对title再做一次完整性检查
                    save_title = utils.fulfill_title(title, selector)
            else:
                if title[-3:] == '...':
                    save_title = utils.fulfill_title(title, selector)
                else:
                    save_title = title
            if self.main_text_pattern:
                main_text, attachment, img = self.parse_main_text(sub_url, request_text)
                self.save_to_data({'title': save_title, 'sub_url': sub_url, 'date': date, 'main_text': main_text,
                                   'source': source, 'attachment': attachment, 'img': img})
            else:
                try:
                    task = parse_context.MAIN_TEXT(sub_url, request_text)
                    result_dict = task.main()
                    main_text = result_dict['content']
                    img = ','.join(result_dict['img'])
                    attachment = ','.join(result_dict['attachment'])
                except Exception:
                    main_text = ''
                    img = ''
                    attachment = ''
                self.save_to_data({'title': save_title, 'sub_url': sub_url, 'date': date, 'main_text': main_text,
                                   'source': source, 'attachment': attachment, 'img': img})
        return frame_list

    async def open_page(self):
        # 网页初始化阶段
        self.page = await self.browser.newPage()
        other_page = await self.browser.pages()
        for page in other_page:
            if page != self.page:
                await page.close()  # 关闭其他无关页面
        await self.page.evaluateOnNewDocument(
            '''() =>{ Object.defineProperties(navigator,{ webdriver:{ get: () => false } }) }''')
        await self.page.evaluateOnNewDocument('''() =>{ window.navigator.chrome = { runtime: {},  }; }''')
        await self.page.evaluateOnNewDocument(
            '''() =>{ Object.defineProperty(navigator, 'languages', { get: () => ['en-US', 'en'] }); }''')
        await self.page.setUserAgent(
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.11 (KHTML, like Gecko) Chrome/23.0.1271.64 Safari/537.11")
        await self.page.setRequestInterception(True)
        self.page.on('request', utils.request_check)  # 不加载图片和媒体等
        self.page.on('dialog', utils.handle_dialog)  # 关闭弹窗
        # 打开网页阶段
        await asyncio.wait([self.page.goto(self.target_url), self.page.waitForNavigation(timeout=CRAWL_SPEED['OPEN_PAGE_MAX_DELAY'])])
        if self.action_pattern:
            # 开始执行网页打开阶段的预执行动作，注意这里的action_type以后可以扩展成多个步骤
            await asyncio.wait([self.page.goto(self.target_url), self.page.waitForNavigation(timeout=CRAWL_SPEED['OPEN_PAGE_MAX_DELAY'])])
            try:
                if self.action_pattern[0] == u'/':
                    button = await self.page.xpath(self.action_pattern)
                else:
                    button = await self.page.xpath('//*[contains(text(),"{}")]'.format(self.action_pattern))
            except Exception:
                return False, u'1找不到网页打开预执行动作的button'
            else:
                if not button:
                    return False, u'1找不到网页打开预执行动作的button'
            try:
                await button[0].click()
                await asyncio.sleep(CRAWL_SPEED['ACTION_DELAY'])
            except Exception as e:
                error_info = u'1网页预执行动作点击过程出现问题 ' + str(e)
                return False, error_info
        return True, ''

    async def get_next_button(self, frame_list, index, page_num):
        # 获取翻页button
        if self.next_patter:
            xpath = self.next_patter
        else:
            xpath = utils.from_xpath_case_to_xpath(self.xpath_case, page_num)
        for frame in [frame_list[index]] + frame_list:
            try:
                button_list = await frame.raw_frame.xpath(xpath)
                # button_list = await self.page.xpath(xpath)
                if not button_list:
                    continue
                else:
                    return button_list[0]
            except Exception:
                self.error_info.append(u'1找不到翻页button')
        return None

    async def get_frame(self):  # frame切换查找
        frame_list = list()
        for frame in self.page.frames:
            my_frame = Frame(frame, self.item_pattern)
            await my_frame.init()
            frame_list.append(my_frame)
        return frame_list

    def find_final_sub_url_list(self, tmp_list):
        # 从各个frame返回的结果里寻找最终的子链接列表,index标识了存有数据表的frame标签
        max_len, index = 0, 0
        final_dict = {}
        if not tmp_list:
            return final_dict, index
        # 按子链接长度和筛选
        for i in range(len(tmp_list)):
            length = sum([len(item) for item in list(tmp_list[i].keys())])
            if length > max_len:
                max_len = length
                final_dict = tmp_list[i]
                index = i
        ret_dict = {title: final_dict[title] for title in list(final_dict.keys()) if
                    title not in self.sub_url_already_crawl}
        return ret_dict, index

    async def turn_page(self, frame_list, page_num, item_list):
        # 翻页
        flag = False
        if self.next_patter:
            xpath = self.next_patter
        else:
            xpath = utils.from_xpath_case_to_xpath(self.xpath_case, page_num)
        js_func = 'result = document.evaluate("{xpath}", document, null, XPathResult.ANY_TYPE, null);' \
                  'node = result.iterateNext();' \
                  'node.target = "";' \
                  'node.click();'.format(xpath=xpath)
        for frame in frame_list:
            try:
                await frame.raw_frame.evaluate(js_func)
            except Exception:
                continue
            else:
                flag = True
                await asyncio.sleep(3)
                break
        if flag:
            await self.wait_for_change(item_list, max_retries=6)
            return True
        else:
            return False

    async def try_to_get_sub_url(self, frame_list):
        tmp_item_list = []
        for frame in frame_list:
            tmp_item_list.append(frame.find_sub_url(bool(self.date_pattern)))
        # index 用来指示含有数据的frame的序号
        new_sub_url, index = self.find_final_sub_url_list(tmp_item_list)
        # if self.mode == 'debug':
        return new_sub_url, index

    def init_links(self, new_links):
        for title in list(new_links.keys()):
            self.sub_url_already_crawl[title] = new_links[title]
            href = new_links[title][0]
            date = new_links[title][1]
            max_id = self.db.select('api_links', 'max(id)', fetch_one=True)['max(id)']
            if not max_id:
                new_id = 1
            else:
                new_id = max_id + 1
            ins_link = {'id': new_id, 'gov': self.gov, 'title': title, 'pub_date': date,
                        'crawl_date': datetime.now().strftime('%Y-%m-%d %X'),
                        'sub_url': href, 'zupei_type': self.zupei_type, 'source': '', 'loc_id': self.loc_id, 'config_id': self.config_id}
            if self.mode == "debug":
                print(ins_link)
            else:
                res = self.db.insert_one('api_links', ins_link)
                if res:
                    self.file_count += 1
                    ins_redis = {'link_id': new_id,
                                 'title': title,
                                 'date': date,
                                 'sub_url': href,
                                 'main_text_pattern': self.main_text_pattern,
                                 'date_pattern': self.date_pattern,
                                 'source_pattern': self.source_pattern,
                                 'title_pattern': self.title_pattern}
                    self.db_redis.sadd('links', str(ins_redis))
                    utils.wait_redis(self.db_redis)
                else:
                    continue

    async def manager(self):
        # 　返回两个值：是否运行成功，翻页页码数
        # 打开页面
        state, error_info = await self.open_page()
        if not state:
            self.error_info.append(error_info)
            return False, 0  # 无法打开页面
        # 获取当前页面子链接
        retry_times = 0  # 轮询计数
        use_browser = False  # 控制是否用浏览器解析子链接
        i = 0  # 循环计数
        while i < self.max_page:
            i += 1
            # 获取frame列表
            frame_list = await self.get_frame()
            # 寻找翻页和子链接
            try:
                new_sub_url, index = await self.try_to_get_sub_url(frame_list)
                print(new_sub_url)
                print(len(new_sub_url))
                sub_urls_copy = copy.deepcopy(new_sub_url)
            except Exception as e:
                error_info = u'1获取子链接过程出错' + str(e)
                self.error_info.append(error_info)
                return False, 0
            else:
                if not new_sub_url:
                    # 翻页前后没变化
                    i -= 1  # 避免页面重复增加
                    if retry_times < 50:
                        retry_times += 1
                        if (i > 0) and (25 < retry_times < 35):
                            c_state = await self.turn_page(frame_list, i + (retry_times - 25), sub_urls_copy)
                            if not c_state:
                                return True, i + 1
                        await asyncio.sleep(1)
                        continue
                    else:
                        return True, i
                else:
                    retry_times = 0
            if i == 1:  # 进入到测试环节
                # 只要还没找到xpath_case, 就寻找一遍，注意寻找的时候遵循原则先从数据框开始
                self.xpath_case = utils.find_xpath_case_in_frames(frame_list, index)
                try:
                    use_browser = utils.test_request(new_sub_url)
                except Exception as e:
                    error_info = u'1检测解析链接方法时出现问题 ' + str(e)
                    self.error_info.append(error_info)
                    use_browser = False
            # 从数据库中过滤掉重复的
            for title in list(new_sub_url.keys()):
                if title in self.sub_url_in_db:
                    # 从数据库删除的同时一定要记得加入already_crawl,避免死循环
                    self.sub_url_already_crawl[title] = new_sub_url[title]
                    new_sub_url.pop(title)
            if self.mode == 'update':  # 如果是增量更新模式的话可以直接退出
                if not new_sub_url:
                    return True, i
            elif self.mode == 'debug':
                print("第{}页".format(str(i)))
            try:
                if use_browser:
                    frame_list = await self.parse_detail(new_sub_url, frame_list, index)
                else:
                    self.init_links(new_sub_url)
            except Exception as e:
                error_info = u'1子链接解析过程存在问题 ' + str(e)
                self.error_info.append(error_info)
                return False, i
            try:
                state = await self.turn_page(frame_list, i, sub_urls_copy)
                if not state:
                    return True, i
            except Exception as e:
                if self.mode == 'debug':
                    print('翻页过程出现错误 ' + str(e))
                return True, i
