#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2021/2/4 下午4:56
# @Author  : Alphasu
# @Function: 利用request发起请求的工具包
import requests
import chardet
import asyncio
from fake_useragent import UserAgent
import aiohttp


def get_proxy(source="http://121.36.18.153:5010/get/"):
    return eval(requests.get(source).text)


class Request:
    def __init__(self, url, cookie='', retry_times=3, timeout=3, allow_redirect=True):
        self.url = url
        self.cookie = cookie
        self.retry_times = retry_times
        self.timeout = timeout
        self.allow_redirect = allow_redirect
        self.ua = UserAgent(verify_ssl=False)
        self.text = ''
        self.status_info = ''
    
    async def get_page_async(self):
        session = aiohttp.ClientSession()
        headers = {
            'User-Agent': "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/73.0.3683.75 Safari/537.36",
        }
        if self.cookie:
            headers['cookie'] = self.cookie
        try:
            response = await session.get(self.url, headers=headers, timeout=16,
                                         allow_redirects=self.allow_redirect)
            # print(response.status)
            if response and response.status == 200:
                encode = chardet.detect(await response.read()).get('encoding', 'utf-8')
                self.text = await response.text(encode, 'ignore')
                self.status_info = '200'
            else:
                # self.get_page(proxy_mode=True)
                self.status_info = str(response.status)
        except Exception as e:
            # self.get_page(proxy_mode=True)
            self.status_info = e
        await session.close()
    
    def get_page(self, proxy_mode=False):
        if proxy_mode: retry = 1
        else: retry = 0
        while retry < self.retry_times:
            headers = {
                'User-Agent': self.ua.random,
            }
            if self.cookie:
                headers['cookie'] = self.cookie
            try:
                if retry == 0:
                    response = requests.get(self.url, headers=headers, timeout=self.timeout, allow_redirects=self.allow_redirect)
                else:
                    # proxy = {'http': get_proxy().get("proxy")}
                    # print(proxy)
                    response = requests.get(self.url, headers=headers, timeout=self.timeout,
                                            allow_redirects=self.allow_redirect)
                if response and response.status_code == 200:
                    encode = chardet.detect(response.content).get('encoding', 'utf-8')  # 通过第3方模块来自动提取网页的编码
                    self.text = response.content.decode(encode, 'ignore')
                    self.status_info = '200'
                    break
                else:
                    self.status_info = str(response.status_code)
            except Exception as e:
                self.status_info = str(e)
            retry += 1


async def main():
    url = 'https://eth.tokenview.com/cn/address/0x1eb2dbfed3d82a5eb758cf53cfe484c40f71cdb1'
    response = await request_async(url)
    print(len(response))


async def request_async(url, timeout=3):
    for i_ in range(15):
        session = aiohttp.ClientSession()
        try:
            if i_ == 0:
                async with session.get(url, allow_redirects=False) as rs:
                    if rs.status == 200:
                        tmp = await rs.text(encoding='utf-8')
                        await session.close()
                        return tmp
                    else:
                        print('connect failure {}'.format(str(i_ + 1)))
            else:
                ua = UserAgent(verify_ssl=False)
                headers = {'User-Agent': ua.random}
                proxy = 'http'+'://' + str(get_proxy().get("proxy"))
                print(proxy)
                async with session.get(url, headers=headers, proxy=proxy, allow_redirects=False, timeout=timeout) as rs:
                    if rs.status == 200:
                        tmp = await rs.text(encoding='utf-8')
                        await session.close()
                        return tmp
                    else:
                        print('connect failure {}'.format(str(i_ + 1)))
        except Exception:
            await session.close()
            print('connect failure {}'.format(str(i_ + 1)))
            continue
        else:
            await session.close()
    return None


if __name__ == '__main__':
    asyncio.run(main())
    # print(get_proxy())
    # request = Request('http://www.dandong.gov.cn/html/57/20182/f3e2bc5bd38d21b9.html')
    # request.get_page()
    # print(request.status_info)
