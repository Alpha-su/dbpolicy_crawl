# dbpolicy_crawl
 社会政策数据库爬虫项目

基于Puppeteer项目的Python接口pyppeteer实现高并发浏览器爬取，通过Redis实现分布式。

爬虫程序入口文件为universe.py，该程序只爬取网页子链接，如需深入解析网页。需启动parse_detail.py

parse_context.py是自动化识别网页正文的文件 

database.py实现了自动化操作数据库的接口
