#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2021/2/4 下午4:56
# @Author  : Alphasu
# @Function: 配置文件
import os

CRAWL_SPEED = {
    'Chromium_Num': 100,  # 控制浏览器的并发数
    'MODE': 'complete',  # 有三种模式运行：complete, debug , update
    'Redis_Stack': 6666,  # 子链接的Redis库最大值
    'Redis_Delay': 6,  # 当子链接的Redis库慢了后的等待时间
    'OPEN_PAGE_MAX_DELAY': 18 * 1000,  # 打开网页的最长时间延迟
    'ACTION_DELAY': 5,  # 执行预执行动作后的固定等待延时
    'CLICK_SUB_URL_MAX_DELAY': 6 * 1000,  # 点击子链接后的最长延时
}

MYSQL = {
    "dbpolicy": '121.36.22.40',
    "user": "root",
    "database": "dbpolicy_web",
    "password": os.environ.get('MYSQL_PWD', '')
}

REDIS = {
    "dbpolicy": "121.36.22.40",
    "password": os.environ.get("REDIS_PWD", '')
}

BROWER = {
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
}


