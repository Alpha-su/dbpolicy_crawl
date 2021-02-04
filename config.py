CRAWL_SPEED = {
    'Chromium_Num': 100,  # 控制浏览器的并发数
    'MODE': 'complete',  # 有三种模式运行：complete, debug , update
    'Redis_Stack': 6666,  # 子链接的Redis库最大值
    'Redis_Delay': 6,  # 当子链接的Redis库慢了后的等待时间
    'OPEN_PAGE_MAX_DELAY': 18 * 1000,  # 打开网页的最长时间延迟
    'ACTION_DELAY': 5,  # 执行预执行动作后的固定等待延时
    'CLICK_SUB_URL_MAX_DELAY': 6 * 1000,  # 点击子链接后的最长延时
}