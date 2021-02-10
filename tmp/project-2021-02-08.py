#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2021/2/8 下午1:32
# @Author  : Alphasu
# @Function: 测试一下新的政策分类效果
import utils
from middleware import ZupeiType


def main():
    insert_list = list()
    db = utils.init_mysql(machine_name='dbpolicy', use_flow=True)
    for i, line in enumerate(db.select('api_links', target='id, title')):
        utils.record_times(i)
        zupei_type_lst = ZupeiType.classification(line['title'])
        for type_ in zupei_type_lst:
            insert_list.append({
                "link_id": line["id"],
                "zupei_type": type_
            })
    db.insert_many('zupei', insert_list)


if __name__ == '__main__':
    main()
    # print(ZupeiType.classification("中共教育部党组关于中国农业大学第二届党委和纪委候选人组成方案的批复"))
