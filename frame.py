import asyncio
import re
from urllib.parse import urljoin
from pyppeteer.errors import NetworkError
from scrapy import Selector
from datetime import date


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


def find_root_node(max_nodes, max_content, sec_nodes):
    for i in range(len(max_nodes) - 1, -1, -1):  # 倒序遍历
        if max_content[i] in sec_nodes:
            return max_nodes[i]
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


class Frame:
    def __init__(self, frame, item_pattern):
        self.raw_frame = frame
        self.url = frame.url
        self.item_pattern = item_pattern
        self.content = None
        self.selector = None
    
    async def init(self):  # 框架初始化
        self.content = await get_content(self.raw_frame)
        self.selector = Selector(text=self.content)
    
    def find_sub_url(self, date_in_detail):
        if self.selector:  # 必须保证初始化成功
            if self.item_pattern:
                new_item = self.find_item_by_config(self.raw_frame.url, date_in_detail)
            else:
                new_item = self.find_item_auto(self.raw_frame.url, date_in_detail)
            return new_item
        else:
            print('frame 初始化失败！')
            return {}
    
    def find_item_by_config(self, url, date_in_detail):
        # 使用配置文件寻找子链接
        ret_dict = dict()
        line_list = self.selector.xpath(self.item_pattern)
        for line in line_list:
            title, sub_link, date = self.solve_one_line(line, url, date_in_detail)
            if not title:
                continue  # find nothing
            else:
                if len(get_chinese(title)) > 3:
                    # 如果是增量更新模式的话，这里还要跟数据库比对
                    ret_dict[title] = (sub_link, date)
        return ret_dict
    
    def find_root_node(self):
        max_len, sec_len = 0, 0
        max_node, second_node = {}, {}
        for node in self.selector.xpath('//a'):
            text, len_text = find_max_text(node)
            if len_text > max_len:
                sec_len = max_len
                max_len = len_text
                second_node = max_node
                max_node = {'text': text, 'node': node}
            elif len_text == max_len:  # 避免max_node和sec_node重复
                continue
            elif len_text > sec_len:
                sec_len = len_text
                second_node = {'text': text, 'node': node}
        if max_len < 5 or (not second_node) or (max_node['text'] == second_node['text']):  # 保证加载完全
            return self.selector
        else:
            max_nodes = max_node['node'].xpath('./ancestor::*')
            max_nodes_content = max_node['node'].xpath('./ancestor::*').extract()
            sec_nodes = second_node['node'].xpath('./ancestor::*').extract()
            root_node = find_root_node(max_nodes, max_nodes_content, sec_nodes)  # 找到距所有节点的最近的父div节点
            return root_node
    
    def find_item_auto(self, url, date_in_detail):
        # 首先要找到两个最长的链接
        ret_dict = {}
        if date_in_detail:  # 如果没有时间约束的话，就不能使用全局root
            root_node = self.find_root_node()
        else:
            root_node = self.selector
        for line in root_node.xpath('.//*'):
            title, sub_link, date = self.solve_one_line(line, url, date_in_detail)
            if not title:
                continue  # find nothing
            else:
                if len(get_chinese(title)) > 3:
                    ret_dict[title] = (sub_link, date)
        return ret_dict
    
    @staticmethod
    def solve_one_line(line, url, date_in_detail):
        """
        从单个信息行中寻找链接文本，地址和包含的时间
        :return:
        :param line: 信息行
        :param url: 当前url
        :return: None if find nothing，tuple(title,sub_link,date) if find something
        """
        tmp_dict = {}
        date = None
        a_list = line.xpath('.//descendant-or-self::a')
        # 寻找line中隐藏的时间
        text_list = line.xpath('.//text()').extract()
        clean_list = [item.strip() for item in text_list if item.strip()]
        for content in clean_list:
            date = search_date_time(content)
            if date:
                break
        if (not date) and (not date_in_detail):  # 检验时间指标的完整性
            return None, None, None
        # 提取子链接，规则为包含中文最多的链接
        for a in a_list:
            text_in_title = a.xpath('./@title').extract()
            text_li = [item.strip() for item in a.xpath('.//text()').extract()]
            if text_li:
                text = text_li[0]
                for tmp in text_li:
                    if len(tmp) > len(text):
                        text = tmp
            else:
                text = ''
            href = a.xpath('./@href').extract_first()
            if href:
                sub_link = urljoin(url, href)
            else:
                sub_link = ''
            if not text_in_title:
                title = text.strip()
            elif len(get_chinese(text_in_title[0])) < len(get_chinese(text)):
                title = text.strip()
            else:
                title = text_in_title[0].strip()
            tmp_dict[title] = sub_link
        final_title_list = list(tmp_dict.keys())
        if not final_title_list:  # 无任何提取到的内容
            return None, None, None
        else:
            final_title = final_title_list[0]
            for title_text in final_title_list:
                tmp1, tmp2 = get_chinese(title_text), get_chinese(final_title)
                if len(tmp1) > len(tmp2):
                    final_title = title_text
            return final_title, tmp_dict[final_title], date  # title, sub_link,date
