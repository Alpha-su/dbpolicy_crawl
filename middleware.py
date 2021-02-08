#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2021/2/8 下午4:30
# @Author  : Alphasu
# @Function: 各种数据处理中的中间件
import re
import utils


# 关于政策分类规则的配置文件
class ZupeiType:
    policy_feature = {"关于", "印发", "通知", "办法", "方案", "意见", "细则", "规定", "计划", "条例", "规范", "制度", "规则", "标准", "试行"}
    category = [
        {
            "name": "领导工作报道",
            "rules": [
                {
                    "need": {"结束", "召开", "闭幕", "开幕", "举办", "会议", "大会", "启动", "讲话", "发布", "宣讲", "培训", "组织", "打响", "调研", "视察", "考察", "督查",
                             "督察", "检查", "应邀", "参观", "出席", "走访", "率队", "带队", "慰问", "活动", "市长", "省长", "局长", "处长", "主任", "厅长", "书记", "院长",
                             "专家", "成员", "班子", "领导", "交流"},
                    "not_need": policy_feature
                },
                {
                    "need": {"参加"},
                    "not_need": policy_feature | {"保险"}
                }
            ]
        },
        {
            "name": "推行举措报道",
            "rules": [
                {
                    "need": {"开展", "打造", "创建", "积极", "推进", "举措", "措施", "着力", "大力", "部署", "倾心", "致力", "落实", "整治", "认真", "学习", "传达", "精神",
                             "做好", "实现", "不断", "创新", "提高", "进一步", "提升", "加强", "严控", "持续", "严厉", "发展", "看病贵", "进展", "强化", "全力", "攻坚", "守护",
                             "维护", "形象", "营造", "良好", "效果", "出炉", "不再", "突破", "取得", "改善", "发生", "推动", "迎来", "确保", "及时", "乱象", "举报", "投诉",
                             "电话", "热线", "抽检"},
                    "not_need": policy_feature
                }
            ]
        },
        {
            "name": "政策成效报道",
            "rules": [
                {
                    "need": {"我", "今年", "目前", "掀起", "再创", "高潮", "热潮", "成果", "佳绩", "成绩", "成效", "辉煌", "大奖", "阶段性", "台阶", "显著", "明显", "亮点",
                             "胜利", "成功", "圆满", "顺利", "完成", "通过", "验收", "宣传", "连续"},
                    "not_need": policy_feature
                },
                {
                    "need": {"全省", "全市"},
                    "not_need": {"要点"} | policy_feature
                }
            ]
        },
        {
            "name": "人民满意度报道",
            "rules": [
                {
                    "need": {"民生", "满意", "群众", "患者", "忧", "难", "心系", "感谢", "百姓", "心声", "人心"},
                    "not_need": policy_feature
                },
                {
                    "need": {"您"},
                    "not_need": None
                }
            ]
        },
        {
            "name": "人事新闻",
            "rules": [
                {
                    "need": {"排名", "称号", "荣", "聘任", "任职", "免职", "招聘", "名单"},
                    "not_need": policy_feature
                },
                {
                    "need": {"表彰", "表扬", "先进单位", "先进集体", "先进个人", "表现突出", "等奖", "任免", "同志"},
                    "not_need": None
                }
            ]
        },
        {
            "name": "统计数据",
            "rules": [
                {
                    "need": {"达", "亿", "万", "元", "人次"},
                    "not_need": policy_feature
                }
            ]
        },
        {
            "name": "政策科普",
            "rules": [
                {
                    "need": {"科普", "知识", "一图读懂", "说明", "记者", "注意", "提醒", "提示", "温馨", "了", "吗", "如何", "？", "，", "“", "”",
                             "（", "）", "！", "【", "】"},
                    "not_need": policy_feature
                },
                {
                    "need": {"解读", "问答", "什么"},
                    "not_need": None
                }
            ]
        },
        {
            "name": "针对个体组织的公告",
            "rules": [
                {
                    "need": {"公司", "广告", "生产", "工程", "扩建", "新建", "改建", "人民医院"},
                    "not_need": policy_feature
                }
            ]
        },
        {
            "name": "疫情分析",
            "rules": [
                {
                    "need": {"确诊病例", "疫情分析", "疫情概况"},
                    "not_need": policy_feature
                }
            ]
        },
        {
            "name": "其他文件体例",
            "rules": [
                {
                    "need": {"公示", "信息公告", "通报", "年报", "申请书", "报告", "总结", "决定书", "行政处罚", "部门决算", "部门预算", "情况", "公开"},
                    "not_need": policy_feature
                },
                {
                    "need": {"统计表"},
                    "not_need": None
                }
            ]
        }
    ]

    @classmethod
    def is_year_in_string(cls, string):
        """
        判断字符串中是否含有年份
        :param string: 任意字符串
        :return: True or False
        """
        year_number = re.findall(r"\d{4}", string)
        for item in year_number:
            if 1949 < int(item) < 2100:
                return True
        return False

    @classmethod
    def classification(cls, title):
        """
        判断当前字符串（标题）的组配分类
        :param title: 标题字符串
        :return: 组配分类列表（一份文件标题可能对应多个分类）
        """
        default_type = [u"政策文件"]
        # 先判断以下是否包含年份信息
        if cls.is_year_in_string(title) and (not utils.is_words_in_string(
                cls.policy_feature | {"法", "要点", "公告", "目录", "规划", "通告", "工作", "预案", "指南", "清单", "政策"}, title)):
            type_lst = [u"年度总结信息"]
        else:
            type_lst = list()
        for type_ in cls.category:
            for rule in type_["rules"]:
                if utils.is_words_in_string(rule["need"], title) and (not utils.is_words_in_string(rule["not_need"], title)):
                    type_lst.append(type_["name"])
                    break
        if type_lst:
            return type_lst
        else:
            return default_type
