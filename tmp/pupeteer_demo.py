import asyncio
from pyppeteer import launch


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


async def main():
    browser = await launch(BROWER)
    page = await browser.newPage()
    await page.goto('http://www.nhc.gov.cn/wjw/wnsj/list.shtml')
    await asyncio.sleep(20)
    await page.screenshot({'path': 'example.png'})
    await browser.close()

if __name__ == '__main__':
    asyncio.get_event_loop().run_until_complete(main())