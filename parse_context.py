import random
from urllib import request
from bs4 import BeautifulSoup
from urllib import parse
import re
import math
import posixpath
import bs4
from urllib.parse import urljoin


def _take_out_list(Data, target_type):
    """拆解嵌套列表"""
    
    def _break_up_list(data, List):
        for item in data:
            if isinstance(item, target_type):
                List.append(item)
            else:
                _break_up_list(item, List)
    
    temporary_list = []
    _break_up_list(Data, temporary_list)
    temporary_list = [i for i in temporary_list if i]
    return temporary_list


def get_attachment_list(soup, url):
    # 传入url是因为有可能需要补全地址
    url_list = []
    a_list = soup.find_all('a')
    for a in a_list:
        pattern1 = re.compile('(.doc|\.docx|\.pdf|\.csv|\.xlsx|\.xls|\.txt)')  # 找到文件后缀
        result1 = pattern1.findall(a.text)
        pattern2 = re.compile('附件')  # 找到附件字样
        result2 = pattern2.findall(a.text)
        pattern3 = re.compile('<a href=(.*)(\.doc|\.docx|\.pdf|\.txt|\.csv|\.xlsx)(.*)</a>')
        result3 = pattern3.findall(str(a))
        if result1 or result2 or result3:
            h = a['href']
            new_url = urljoin(url,h)
            url_list.append(new_url)
    return url_list


def get_user_agents():
    user_agents = ['Mozilla/5.0 (Windows NT 6.1; WOW64; rv:23.0) Gecko/20130406 Firefox/23.0',
                   'Mozilla/5.0 (Windows NT 6.1; WOW64; rv:18.0) Gecko/20100101 Firefox/18.0',
                   'Mozilla/5.0 (Windows; U; Windows NT 6.1; en-US) AppleWebKit/533+ \
                   (KHTML, like Gecko) Element Browser 5.0',
                   'IBM WebExplorer /v0.94', 'Galaxy/1.0 [en] (Mac OS X 10.5.6; U; en)',
                   'Mozilla/5.0 (compatible; MSIE 10.0; Windows NT 6.1; WOW64; Trident/6.0)',
                   'Opera/9.80 (Windows NT 6.0) Presto/2.12.388 Version/12.14',
                   'Mozilla/5.0 (iPad; CPU OS 6_0 like Mac OS X) AppleWebKit/536.26 (KHTML, like Gecko) \
                   Version/6.0 Mobile/10A5355d Safari/8536.25',
                   'Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36 (KHTML, like Gecko) \
                   Chrome/28.0.1468.0 Safari/537.36',
                   'Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.0; Trident/5.0; TheWorld)']
    index = random.randint(0, 9)
    user_agent = user_agents[index]
    return user_agent


def download(attachment_list):
    for attachment_list_url in attachment_list:
        print(attachment_list_url)
        houzhui = get_houzhui(attachment_list_url)
        opener = request.build_opener()
        opener.addheaders = [('User-agent', get_user_agents())]
        request.install_opener(opener)
        try:
            request.urlretrieve(attachment_list_url, 'path + file_name' + houzhui)
            # path为本地保存路径，file_name为文件名
            print('下载附件成功')
        except:
            print("下载附件失败")


def get_houzhui(str):
    strlist = str.split('/')
    i = len(strlist)
    str2 = '.' + strlist[i - 1].split('.')[-1]  # 获取最后一个元素
    if str2 in ['.doc', '.docx', '.xlsx', '.xls', '.zip', '.txt', '.ppt', '.pptx', '.rar', '.bmp',
                '.pic', '.avi', '.wav', '.bat', '.gif', '.html', '.htm', '.jpeg', '.jpg', '.mp3', '.pdf', '.png']:
        return str2
    else:
        return ''


def remove_js_css(content):
    """ remove the the javascript and the stylesheet and the comment content
    (<script>....</script> and <style>....</style> <!-- xxx -->) """
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


def remove_empty_line(content):
    """remove multi space """
    r = re.compile(r'''''^\s+$''', re.M | re.S)
    s = r.sub('', content)
    r = re.compile(r'''''\n+''', re.M | re.S)
    s = r.sub('\n', s)
    return s


def remove_li_tag(content):
    r = re.compile(r'<li.*?</li>', re.I | re.M | re.S)
    s = r.sub('', content)
    return s


def _find_info_list(content_tag):
    """由正文节点向前寻找info_list"""
    previous = content_tag.find_all_previous()
    for brother_tag in previous:
        title_list = brother_tag.find_all('ul') + brother_tag.find_all('table')
        if title_list:
            title = re.sub('\s+', ' ', title_list[0].text)
            if title:
                return title
    return None


class MAIN_TEXT:
    def __init__(self, url, text, separator="\n", keep_gif=False, smallest_length=2, word_with_format=False,
                 img_with_format=True, shortest_length=18, encoding=None, with_date=False):
        self.url = url
        self.text = text
        self.separator = separator
        self.keep_gif = keep_gif
        self.smallest_length = smallest_length
        self.word_with_format = word_with_format
        self.img_with_format = img_with_format
        self.shortest_length = shortest_length
        self.encoding = encoding
        self.with_date = with_date
        self.elements = {
            "state": 1,
            'content': '',
            'img': list(),
            'attachment': list(),
            'error': ''
        }
    
    regexps = {
        "unlikelyCandidates": re.compile(r"combx|comment|community|disqus|extra|foot|header|enu|remark|rss|shoutbox|"
                                         r"sidebar|sponsor|ad-break|agegate|pagination|pager|popup|tweet|twitter"),
        "okMaybeItsACandidate": re.compile(r"and|article|body|column|main|shadow"),
        "positive": re.compile(r"article|body|content|entry|hentry|main|page|pagination|post|text|blog|story|view"),
        "negative": re.compile(r"combx|comment|com|contact|foot|footer|footnote|masthead|media|meta|outbrain|promo|"
                               r"related|scroll|shoutbox|sidebar|sponsor|shopping|tags|tool|widget"),
        "extraneous": re.compile(r"print|archive|comment|discuss|e[\-]?mail|share|reply|all|login|sign|single"),
        "divToPElements": re.compile(r"<(a|blockquote|dl|div|img|ol|p|pre|table|ul)"),
        "trim": re.compile(r"^\s+|\s+$"),
        "normalize": re.compile(r"\s{2,}"),
        "videos": re.compile(r"http://(www\.)?(youtube|vimeo)\.com"),
        "skipFootnoteLink": re.compile(r"^\s*(\[?[a-z0-9]{1,2}\]?|^|edit|citation needed)\s*$"),
        "nextLink": re.compile(r"(next|weiter|continue|>([^|]|$)|»([^|]|$))"),
        "prevLink": re.compile(r"(prev|earl|old|new|<|«)"),
        "url": re.compile(
            r'(?i)\b((?:[a-z][\w-]+:(?:/{1,3}|[a-z0-9%])|www\d{0,3}[.]|[a-z0-9.\-]+[.][a-z]{2,4}/)(?:'
            r'[^\s()<>]+|\(([^\s()<>]+|(\([^\s()<>]+\)))*\))+(?:\(([^\s()<>]+|(\([^\s()<>]+\)))*\)|'
            r'[^\s`!()\[\]{};:\'".,<>?«»“”‘’]))'),
        "brackets": re.compile(r"<.*?>"),
        "symbol": re.compile(r"\r|&gt;|\xa0"),
        "chinese": re.compile(u"[\u4e00-\u9fa5]*"),
        "title": re.compile(r'<h[1-3].*'),
        "info_list": re.compile(r'<ul.*'),
        "date": re.compile(
            r'(20[0-2][0-9]|[0-1][0-9])[^a-zA-Z0-9](1[0-2]|0?[0-9])[^a-zA-Z0-9](3[0-1]|2[0-9]|1[0-9]|0?[0-9]).?')
    }
    
    re_date_list = ["(\d{4}[-|/|.]\d{1,2}[-|/|.]\d{1,2}\s?[0-1]?[0-9]:[0-5]?[0-9]:[0-5]?[0-9])",
                    "(\d{4}[-|/|.]\d{1,2}[-|/|.]\d{1,2}\s?2:[0-5]?[0-9]:[0-5]?[0-9])",
                    "(\d{4}[-|/|.]\d{1,2}[-|/|.]\d{1,2}\s?[0-1]?[0-9]:[0-5]?[0-9])",
                    "(\d{4}[-|/|.]\d{1,2}[-|/|.]\d{1,2}\s?2:[0-5]?[0-9])",
                    "(\d{4}[-|/|.]\d{1,2}[-|/|.]\d{1,2}\s?[1-24]\d时[0-60]\d分)([1-24]\d时)",
                    "(\d{2}[-|/|.]\d{1,2}[-|/|.]\d{1,2}\s?[0-1]?[0-9]:[0-5]?[0-9]:[0-5]?[0-9])",
                    "(\d{2}[-|/|.]\d{1,2}[-|/|.]\d{1,2}\s?2:[0-5]?[0-9]:[0-5]?[0-9])",
                    "(\d{2}[-|/|.]\d{1,2}[-|/|.]\d{1,2}\s?[0-1]?[0-9]:[0-5]?[0-9])",
                    "(\d{2}[-|/|.]\d{1,2}[-|/|.]\d{1,2}\s?2:[0-5]?[0-9])",
                    "(\d{2}[-|/|.]\d{1,2}[-|/|.]\d{1,2}\s?[1-24]\d时[0-60]\d分)([1-24]\d时)",
                    "(\d{4}年\d{1,2}月\d{1,2}日\s?[0-1]?[0-9]:[0-5]?[0-9]:[0-5]?[0-9])",
                    "(\d{4}年\d{1,2}月\d{1,2}日\s?2:[0-5]?[0-9]:[0-5]?[0-9])",
                    "(\d{4}年\d{1,2}月\d{1,2}日\s?[0-1]?[0-9]:[0-5]?[0-9])",
                    "(\d{4}年\d{1,2}月\d{1,2}日\s?2:[0-5]?[0-9])",
                    "(\d{4}年\d{1,2}月\d{1,2}日\s?[1-24]\d时[0-60]\d分)([1-24]\d时)",
                    "(\d{2}年\d{1,2}月\d{1,2}日\s?[0-1]?[0-9]:[0-5]?[0-9]:[0-5]?[0-9])",
                    "(\d{2}年\d{1,2}月\d{1,2}日\s?2:[0-5]?[0-9]:[0-5]?[0-9])",
                    "(\d{2}年\d{1,2}月\d{1,2}日\s?[0-1]?[0-9]:[0-5]?[0-9])",
                    "(\d{2}年\d{1,2}月\d{1,2}日\s?2:[0-5]?[0-9])",
                    "(\d{2}年\d{1,2}月\d{1,2}日\s?[1-24]\d时[0-60]\d分)([1-24]\d时)",
                    "(\d{1,2}月\d{1,2}日\s?[0-1]?[0-9]:[0-5]?[0-9]:[0-5]?[0-9])",
                    "(\d{1,2}月\d{1,2}日\s?2:[0-5]?[0-9]:[0-5]?[0-9])",
                    "(\d{1,2}月\d{1,2}日\s?[0-1]?[0-9]:[0-5]?[0-9])",
                    "(\d{1,2}月\d{1,2}日\s?2:[0-5]?[0-9])",
                    "(\d{1,2}月\d{1,2}日\s*?[1-24]\d时[0-60]\d分)([1-24]\d时)",
                    "(\d{4}[-|/|.]\d{1,2}[-|/|.]\d{1,2})",
                    "(\d{2}[-|/|.]\d{1,2}[-|/|.]\d{1,2})",
                    "(\d{4}年\d{1,2}月\d{1,2}日)",
                    "(\d{2}年\d{1,2}月\d{1,2}日)",
                    "(\d{1,2}月\d{1,2}日)"]
    
    def main(self):
        # 返回值的state，为0表示出错，为1表示正常，为2表示正文没有内容但有附件或图片
        page1 = remove_js_css(str(self.text))
        page1 = remove_empty_line(page1)
        bsobj = BeautifulSoup(page1, "html.parser")
        
        # 寻找附件地址
        try:
            attachment_list = get_attachment_list(bsobj, self.url)
        except Exception as e:
            attachment_list = []
            print('error in parse_context.main(): ' + str(e))
        
        self.elements['attachment'] = attachment_list
        alternative_dict = {}
        for tag in bsobj.body.find_all(True):
            if tag.name == "p":  # 如果节点是p标签，找到字符和向上两层节点
                parent_tag = tag.parent
                grandparent_tag = parent_tag.parent
                inner_text = tag.text
                if not parent_tag or len(inner_text) < 20:  # 如果该节点为空或无有价值内容
                    continue
                parent_hash = hash(str(parent_tag))  # 内容太多放不进字典，计算字符串哈希值以取唯一值
                grand_parent_hash = hash(str(grandparent_tag))
                if parent_hash not in alternative_dict:  # 如果该节点内有内容，放入向上两层节点内容和分数
                    alternative_dict[parent_hash] = self._tag_score(parent_tag)
                if grandparent_tag and grand_parent_hash not in alternative_dict:
                    alternative_dict[grand_parent_hash] = self._tag_score(grandparent_tag)
                # 计算此节点分数，以逗号和长度作为参考，并使向上两层递减获得加权分
                content_score = 1
                content_score += inner_text.count(",")
                content_score += inner_text.count(u"，")
                content_score += inner_text.count("。")
                content_score += min(math.floor(len(inner_text) / 100), 3)
                alternative_dict[parent_hash]["score"] += content_score
                if grandparent_tag:
                    alternative_dict[grand_parent_hash]["score"] += content_score / 2
        
        best_tag = None
        for key in alternative_dict:
            alternative_dict[key]["score"] *= 1 - self._link_score(alternative_dict[key]["tag"])
            if not best_tag or alternative_dict[key]["score"] > best_tag["score"]:
                best_tag = alternative_dict[key]
        if not best_tag:
            if not attachment_list:
                self.elements['state'] = 0
            else:
                self.elements['state'] = 2
            self.elements['error'] = "Couldn't find the optimal node"
            return self.elements
        
        content_tag = best_tag["tag"]
        
        # 对最优节点格式清洗
        for tag in content_tag.find_all(True):
            del tag["class"]
            del tag["id"]
            del tag["style"]
        # 清理标签，清理无用字段
        content_tag = self._clean(content_tag, "h1")
        content_tag = self._clean(content_tag, "object")
        alternative_dict, content_tag = self._clean_alternative_dict(content_tag, "form", alternative_dict)
        if len(content_tag.find_all("h2")) == 1:
            content_tag = self._clean(content_tag, "h2")
        content_tag = self._clean(content_tag, "iframe")
        alternative_dict, content_tag = self._clean_alternative_dict(content_tag, "table", alternative_dict)
        alternative_dict, content_tag = self._clean_alternative_dict(content_tag, "ul", alternative_dict)
        alternative_dict, content_tag = self._clean_alternative_dict(content_tag, "div", alternative_dict)
        
        # 找寻图片地址
        imgs = content_tag.find_all("img")
        # 得到所有地址，清理无用地址
        for img in imgs:
            src = img.get("src", None)
            if not src:
                img.extract()
                continue
            elif "http://" != src[:7] and "https://" != src[:8]:
                newSrc = parse.urljoin(self.url, src)
                newSrcArr = parse.urlparse(newSrc)
                newPath = posixpath.normpath(newSrcArr[2])
                newSrc = parse.urlunparse((newSrcArr.scheme, newSrcArr.netloc, newPath,
                                           newSrcArr.params, newSrcArr.query, newSrcArr.fragment))
                img["src"] = newSrc
        content_tag = self._clean(content_tag, "img")
        
        # 正文内中文内容少于设定值，默认定位失败
        content_text = content_tag.get_text(strip=True, separator=self.separator)
        content_length = len("".join(self.regexps["chinese"].findall(content_text)))
        if content_length <= self.shortest_length:
            self.elements['img'] = [urljoin(self.url, tag.get("src")) for tag in bsobj.body.find_all('img')]
            if self.elements['attachment'] or self.elements['img']:
                # 图片或附件只要有一个存在的
                self.elements['state'] = 2  # 没有正文，但是有附件或者图片
            else:
                self.elements['state'] = 0
            self.elements['error'] = "Page is empty or without content"
            return self.elements
        
        content = self._parameter_correction(content_tag)
        self.elements['content'] = content
        self.elements['img'] = self.img
        return self.elements
    
    def _tag_score(self, tag):
        """加权框架分计算"""
        score = 0
        if tag.name == "div":
            score += 5
        elif tag.name == "blockquote":
            score += 3
        elif tag.name == "form":
            score -= 3
        elif tag.name == "th":
            score -= 5
        score += self._class_score(tag)
        return {"score": score, "tag": tag}
    
    def _class_score(self, tag):
        """加权类分计算"""
        score = 0
        if "class" in tag:
            if self.regexps["negative"].search(tag["class"]):
                score -= 25
            elif self.regexps["positive"].search(tag["class"]):
                score += 25
        if "id" in tag:
            if self.regexps["negative"].search(tag["id"]):
                score -= 25
            elif self.regexps["positive"].search(tag["id"]):
                score += 25
        return score
    
    @staticmethod
    def _link_score(tag):
        """加权标签内部分数"""
        links = tag.find_all("a")
        textLength = len(tag.text)
        if textLength == 0:
            return 0
        link_length = 0
        for link in links:
            link_length += len(link.text)
        return link_length / textLength
    
    def _clean(self, content, tag):
        """清理符合条件的标签"""
        target_list = content.find_all(tag)
        flag = False
        if tag == "object" or tag == "embed":
            flag = True
        for target in target_list:
            attribute_values = ""
            for attribute in target.attrs:
                get_attr = target.get(attribute[0])
                attribute_values += get_attr if get_attr is not None else ""
            if flag and self.regexps["videos"].search(attribute_values) \
                    and self.regexps["videos"].search(target.encode_contents().decode()):
                continue
            target.extract()
        return content
    
    def _clean_alternative_dict(self, content, tag, alternative_dict):
        """字典计分加权以清理无用字段"""
        tags_list = content.find_all(tag)
        # 对每一节点评分并调用存档评分
        for tempTag in tags_list:
            score = self._class_score(tempTag)
            hash_tag = hash(str(tempTag))
            if hash_tag in alternative_dict:
                content_score = alternative_dict[hash_tag]["score"]
            else:
                content_score = 0
            # 清理负分节点
            if score + content_score < 0:
                tempTag.extract()
            else:
                p = len(tempTag.find_all("p"))
                img = len(tempTag.find_all("img"))
                li = len(tempTag.find_all("li")) - 100
                input_html = len(tempTag.find_all("input_html"))
                embed_count = 0
                embeds = tempTag.find_all("embed")
                # 如果找到视频，考虑删除节点
                for embed in embeds:
                    if not self.regexps["videos"].search(embed["src"]):
                        embed_count += 1
                linkscore = self._link_score(tempTag)
                contentLength = len(tempTag.text)
                toRemove = False
                # 删除节点逻辑
                if img > p:
                    toRemove = True
                elif li > p and tag != "ul" and tag != "ol":
                    toRemove = True
                elif input_html > math.floor(p / 3):
                    toRemove = True
                elif contentLength < 25 and (img == 0 or img > 2):
                    toRemove = True
                elif score < 25 and linkscore > 0.2:
                    toRemove = True
                elif score >= 25 and linkscore > 0.5:
                    toRemove = True
                elif (embed_count == 1 and contentLength < 35) or embed_count > 1:
                    toRemove = True
                # 逻辑成立则删除节点
                if toRemove:
                    tempTag.extract()
        return alternative_dict, content
    
    def _parameter_correction(self, content):
        """依据选择参数的调整格式"""
        content_tag_list = []
        for tag in content:
            if not isinstance(tag, bs4.element.Tag):
                continue
            if "<img" in tag.decode():
                content_tag_list.extend(tag.find_all("img"))
            else:
                content_tag_list.append(tag)
        self.img = [urljoin(self.url, tag.get("src")) for tag in content_tag_list if tag.name == "img"]
        # 对于各种参数的选择，原地清理列表并筛选列表
        if not self.word_with_format:
            for v in range(len(content_tag_list)):
                if isinstance(content_tag_list[v], bs4.element.Tag):
                    if content_tag_list[v].name == 'img':
                        src = content_tag_list[v].get("src")
                        if not self.keep_gif and ('.gif' in src or '.GIF' in src):
                            src = None
                        if self.img_with_format and src:
                            src = '<img src="' + src + '"/>'
                        content_tag_list[v] = src
                    else:
                        if isinstance(content_tag_list[v], bs4.element.NavigableString):
                            content_tag_list[v] = content_tag_list[v].string
                        content_tag_list[v] = content_tag_list[v].get_text(strip=True)
                        content_tag_list[v] = self.regexps["symbol"].sub("", content_tag_list[v])
                        if len("".join(self.regexps["chinese"].findall(content_tag_list[v]))) < self.smallest_length:
                            content_tag_list[v] = None  # 清理每段低于最小长度的文字节点
        content_tag_list = filter(lambda x: x, content_tag_list)
        content_tag_list = list(map(lambda x: str(x), content_tag_list))
        content = self.separator.join(content_tag_list)
        return content
    
    def _find_title(self, content_tag):
        """由正文节点向前寻找标题（h1-h3)"""
        previous = content_tag.find_all_previous()
        for brother_tag in previous:
            title_list = self.regexps["title"].findall(str(brother_tag))
            if title_list:
                title = self.regexps['brackets'].sub("", title_list[0])
                if title:
                    return title
        return None
    
    def _find_date(self, content_tag):
        """由正文节点向前寻找时间
        注意，此模块尚未完善，谨慎使用！
        这个比较麻烦，一方面网上流传的正则表达式很多都无法使用，另一方面不同模板的日期格式各有不同，逻辑往往是互斥的
        因此在简单正则逻辑的基础上，加入投票的概念。"""
        date_list = []
        previous = content_tag.find_all_previous()
        for brother_tag in previous:
            date = self.regexps["date"].search(str(brother_tag))
            if not date:
                for re_date in self.re_date_list:
                    date1 = re.compile(re_date).search(str(brother_tag))
                    if date1:
                        date = date1
            if date:
                date_list.append(date.group())
        if date_list:
            date_list = [[x, date_list.count(x)] for x in date_list]
            date_list.sort(key=lambda x: x[1], reverse=True)
            self.date = date_list[0][0].strip(" <\t\r\n")


# 示例
if __name__ == "__main__":
    task = MAIN_TEXT(url=r"http://www.nhc.gov.cn/yzygj/s3590/202002/d5d3a75ab16a4028919ad5ce2f975d99.shtml",
                     with_date=True)
    print(task.main())
    # from R2 import Request
    # print(Request('http://amr.ah.gov.cn/public/11/913681.html').response.text)
