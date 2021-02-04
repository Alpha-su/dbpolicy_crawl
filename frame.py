#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2021/2/4 下午4:56
# @Author  : Alphasu
# @Function: 解析网页frame，从网页frame中提取子链接
from urllib.parse import urljoin
from scrapy import Selector
import utils


def find_root_node(max_nodes, max_content, sec_nodes):
    for i in range(len(max_nodes) - 1, -1, -1):  # 倒序遍历
        if max_content[i] in sec_nodes:
            return max_nodes[i]
    return None


class Frame:
    def __init__(self, frame, item_pattern):
        self.raw_frame = frame
        self.url = frame.url
        self.item_pattern = item_pattern
        self.content = None
        self.selector = None
    
    async def init(self):  # 框架初始化
        self.content = await utils.get_content(self.raw_frame)
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
                if len(utils.get_chinese(title)) > 3:
                    # 如果是增量更新模式的话，这里还要跟数据库比对
                    ret_dict[title] = (sub_link, date)
        return ret_dict
    
    def find_root_node(self):
        max_len, sec_len = 0, 0
        max_node, second_node = {}, {}
        for node in self.selector.xpath('//a'):
            text, len_text = utils.find_max_text(node)
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
                if len(utils.get_chinese(title)) > 3:
                    ret_dict[title] = (sub_link, date)
        return ret_dict
    
    @staticmethod
    def solve_one_line(line, url, date_in_detail):
        """
        从单个信息行中寻找链接文本，地址和包含的时间
        :param date_in_detail: 传入的date位置的配置文件
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
            date = utils.search_date_time(content)
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
            elif len(utils.get_chinese(text_in_title[0])) < len(utils.get_chinese(text)):
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
                tmp1, tmp2 = utils.get_chinese(title_text), utils.get_chinese(final_title)
                if len(tmp1) > len(tmp2):
                    final_title = title_text
            return final_title, tmp_dict[final_title], date  # title, sub_link,date
