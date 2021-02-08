#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2021/2/4 下午4:56
# @Author  : Alphasu
# @Function: 一些合并时候用到的临时代码
import csv
from request_tools import Request
import database
import parse_context


def set_in_title(title):
    tmp_set = {u"失业", u"就业", u"创业", u"劳动", u"职业", u"岗位"}
    for item in tmp_set:
        if item in title:
            return True
    return False


def fulfill():
    with open('search_result.csv','r',encoding='utf-8-sig',newline='') as fr:
        csv_r = csv.reader(fr)
        for line in csv_r:
            print(line)
            url = line[-1]
            response = Request(url).text
            if not response:line.append('')
            else:
                task = parse_context.MAIN_TEXT(url,response)
                try:
                    result = task.main()
                    line.append(result['content'])
                except Exception:
                    line.append('')
            with open('search_result1.csv','a',encoding='utf-8-sig') as fa:
                csv_a = csv.writer(fa)
                csv_a.writerow(line)
            

def get_from_local():
    db = database.Mysql('root', '990211', 'data_policy')
    matrix = db.select('data')
    for line in matrix:
        code = line[0]
        result = db.select('map_location',condition='code="{}"'.format(code),fetch_one=True)
        province = result[1]
        city = result[2]
        insert_list = [code,province,city] + list(line)[1:]
        with open('local_result.csv','a',encoding='utf-8-sig',newline='') as fa:
            csv_a = csv.writer(fa)
            csv_a.writerow(insert_list)


def update_from_local():
    db = database.Mysql('root', '990211', 'dbpolicy')
    with open('local_result.csv','r',encoding='utf-8-sig') as fr:
        csv_r = csv.reader(fr)
        for line in csv_r:
            insert_dict = {'code':line[0],'province':line[1],'city':line[2],'gov':line[3],'title':line[4],'date':line[5],'sub_url':line[6]}
            response = Request(insert_dict['sub_url']).text
            if not response:
                continue
            else:
                task = parse_context.MAIN_TEXT(insert_dict['sub_url'], response)
                try:
                    result = task.main()
                    insert_dict['main_text'] = result['content']
                    insert_dict['attachment'] = ','.join(result['attachment'])
                    insert_dict['img'] = ','.join(result['img'])
                except Exception:
                    continue
            db.insert_one('data',insert_dict)


def is_satisfied(title):
    ret_list = set()
    avi_set = {u'社会救助',u'最低生活保障',u'低保'}
    avi_dict = {U'办法':1,u'条例':2,u'意见':3,u'制度':4,u'认定':5, u'审核审批':6,u'审批权':7,u'家庭经济状况':8,u'信息系统':9,
                u'专项治理':10,'加强':11,u'改进':11,u'检查':12,u'绩效评价':13,u'经办服务':14,u'资金':15,u'兜底':16}
    if u'低收入家庭认定' in title:
        ret_list.add('17')
    elif u'低收入家庭经济状况核对' in title:
        ret_list.add('18')
    elif u'居民家庭经济状况核对' in title:
        ret_list.add('19')
    else:
        for item in avi_set:
            if item in title:
                for key in list(avi_dict.keys()):
                    if key in title:
                        ret_list.add(str(avi_dict[key]))
    return ret_list
    

def search():
    res_dict = list()
    db = database.Mysql('root','990211','dbpolicy',host='121.36.22.40',use_flow=True)
    for policy in db.select('data'):
        title = policy['title']
        avi_label = is_satisfied(title)
        if avi_label:
            label = ' '.join(avi_label)
            policy['label'] = label
            res_dict.append(policy)
    with open('data_with_label.csv','w',encoding='utf-8-sig', newline='') as fw:
        c_w = csv.DictWriter(fw, list(res_dict[0].keys()))
        c_w.writeheader()
        c_w.writerows(res_dict)

def merge_from_others():
    # db_host = database.Mysql('root','990211','data_policy',use_flow=True)
    # db_host_r = database.Mysql('root', '990211', 'data_policy')
    db_sever_w = database.Mysql('root','990211','dbpolicy',host='121.36.22.40')
    db_sever_r = database.Mysql('root','990211','dbpolicy',host='121.36.22.40',use_flow=True)
    i = 0
    for line in db_sever_r.select('data'):
        if (i % 10 == 0):print(i)
        i += 1
        if not line['gov']:
            gov = '人民政府'
        else:
            gov = line['gov']
        if line['date']:
            date = line['date']
        else:
            date = None
        insert_dict = {'code':line['code'],'province':line['province'],'city':line['city'],
                        'title':line['title'],'gov':gov,'date':date,'sub_url':line['sub_url']}
        db_sever_w.insert_one('links',insert_dict)

    # for item in db_sever_r.select('data'):
    #     insert_links = {'code':item['code'],'title':item['title'],'province':item['province'],'city':item['city'],'gov':item['gov'],
    #                     'date':item['date'],'sub_url':item['sub_url'],'target_url':item['target_url']}
    #     insert_detail = {'code':item['code'],'title':item['title'],'sub_url':item['sub_url'],'main_text':item['main_text'],
    #                      'zupei_type':item['zupei_type'],'zhuti_type':item['zhuti_type'],'ticai_type':item['ticai_type'],
    #                      'key_words':item['key_words'],'attachment':item['attachment'],'img':item['img'],'rank':item['rank']}
    #     db_sever_w.insert_one('links',insert_links)
    #     db_sever_w.insert_one('detail',insert_detail)

def test_url(url):
    response = Request(url).response
    return bool(response)


def migrations():
    db = database.Mysql('root','990211','dbpolicy_web',host='121.36.22.40')
    with open('./data/configure_gov_v2.csv', 'r', encoding='utf-8') as fr:
        csv_r = csv.DictReader(fr)
        id = 0
        for line in csv_r:
            id += 1
            loc_id = db.select('api_location',"id","code={}".format(line['\ufeffcode']),fetch_one=True)['id']
            insert_config = {
                "id": id,
                'gov':line["gov"],
                'target_url':line['target_url'],
                'is_active':test_url(line['target_url']),
                "item_pattern":line["item_pattern"],
                "main_text_pattern":line["main_text_pattern"],
                "date_pattern":line['date_type'],
                "zupei_pattern":line['zupei_type'],
                "source_pattern":line['source'],
                'title_pattern':line['title'],
                'next_pattern':line['next_button'],
                'action_pattern':line['action'],
                'loc_id':loc_id,
                'file_count':0,
                'author_id':1,
            }
            res = db.insert_one('api_config',insert_config)
            if not res:
                id -= 1


if __name__ == '__main__':
    # search()
    # merge_from_others()
    # migrations()
    # get_from_local()
    # update_from_local()
    # migrant()
    db = database.Mysql('root','990211','dbpolicy_web',host='121.36.22.40')
    for line in db.select('api_config',condition="is_active=0"):
        url = line['target_url']
        response = Request(url, timeout=10).text
        if response:
            print(line)
            db.update('update api_config set is_active=1 where id = "{}"'.format(line['id']))
        else:
            db.delete('api_config','id="{}"'.format(line['id']))