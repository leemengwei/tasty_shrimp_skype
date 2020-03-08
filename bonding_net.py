import os,sys,time
import pandas as pd
from IPython import embed
import re
import glob
import tqdm
import extract_msg
#import openpyxl
import xlrd


README = \
'''
**************************
*整体使用逻辑，非开发逻辑*
1、获取任意cargo  X
2、AXS输入筛选条件   X
3、最近2-3天更新的船  X
4、ETA排序  X
5、导出表格  X
6、删除ETA不符合要求的船   X
7、执行Ballast（距离）排序   X
8、执行人工所在POSITION的筛选   X
9、获取Skype id/电话/Personal Email等---->related
10、生成该cargo发送/联系人信息---->related
11、对Email源地址群发标准格式邮件---->X
12、对Skype/Personal Email导入船名和货物，点对点发送  X
13、检测回复、促成Fixutre   X
14、获取丰厚佣金   X

开发逻辑：有关9&10.
1、逐个解析所有msg；
2、逐个找出msg发件人；
3、区分发件人，通过纽带筛选direct；
4、匹配所有direct的船名；
5、匹配所有direct的skype与邮件；
6、生成该blob；
*************************************

'''


def parse_msg(msg_file_path):
    f = msg_file_path  # Replace with yours
    msg = extract_msg.Message(f)
    #msg_sender = msg.sender
    #msg_date = msg.date
    #msg_subj = msg.subject
    msg_content = msg.body
    if DEBUG:
        print("Got msg:\n", "~"*24)
        #print(msg_content[:50])
        print("~"*30)
    return msg, msg_content

def get_sender_email(msg):
    sender_email = msg.sender
    if DEBUG:
        print("Got sender:")
        print(sender_email)
    return sender_email

def judge_if_direct_counterpart(sender_email):
    direct_counterpart = True
    return direct_counterpart

def retrieve_vessel(msg_content, vessels_c):
    vessels_name = vessels_c.findall(msg_content)
    if DEBUG:
        print("Got vessels name:")
        print(vessels_name)
    return vessels_name

def retrieve_skype(msg_content):
    tmp = re.compile(r'skype', re.I)
    tmp_id = tmp.search(msg_content)
    if tmp_id:
        print("\n"+msg_content[tmp_id.span()[0]-15: tmp_id.span()[1]+15])
    #Skype : xxxx
    pattern1 = '[\(]?skype[\)]?[ ]?[ ]?[ ]?[:]?([ 0-9a-z_\-:\.@]+)'
    #skype id : xxx
    pattern2 = 'skype[ ]*id[ ]*:([ 0-9a-z_\-:\.@]+)'
    #skpye msn :xx
    pattern3 = 'skype/msn[ ]*:([ 0-9a-z_\-:\.@]+)'
    all_patterns = '|'.join([pattern1, pattern2, pattern3])
    c = re.compile(all_patterns, re.I)
    skypes_id = c.findall(msg_content)
    if tmp_id:
        print("Got skype:")
        print(skypes_id)
    return skypes_id

def parse_blob(vessels_name, sender_email, skypes_id):
    blob = []
    if DEBUG:
        print("Got blob:", blob)
    return blob

def get_counterparts_repository():
    #couterparts_repository = open(DATA_PATH_PREFIX+"/couterparts_repository.txt", 'r').readlines()
    couterparts_repository = []
    return couterparts_repository

def get_vessels_repository():
    workbook = xlrd.open_workbook(DATA_PATH_PREFIX+"/vessels_repository.xlsx")
    vessels_repository_raw = []
    for sheet in workbook.sheet_names():
        table = workbook.sheet_by_name(sheet)
        vessels_repository_raw += table.col_values(1)[1:]
    vessels_repository = list(set(vessels_repository_raw))
    vessels_repository.sort(key=vessels_repository_raw.index)
    safe_name = []
    unsafe_name = []
    for vessel in vessels_repository:
        if ' ' in vessel or '.' in vessel or '-' in vessel:
            safe_name.append(vessel)
        else:
            unsafe_name.append(vessel)
    #Use raw name for safe:
    head = '('
    tail = ')[^A-Za-z0-9]'
    pattern1 = head+'|'.join(safe_name)+tail
    #Mv m.v m/v added for unsafe:
    head = '[M|m][.|/]?[V|v][.|/:]? [\'|\"]?('
    tail = ')[^A-Za-z0-9]'
    pattern2 = head+'|'.join(unsafe_name)+tail
    #Porpose propse purpose pps ppse offer for:
    head = '[Pp][Uu|Rr]?[Rr|Oo]*[Pp][Oo]?[Ss][Ee]?|[Oo]ffer|OFFER [Ff][Oo][Rr][ :\\n\\r\\t]*[\'|\"]?('
    tail = ')[^A-Za-z0-9]'
    pattern3 = head+'|'.join(unsafe_name)+tail
    #Not in repository:  mv
    extra_MV_patterns = '[M|m][.|/]?[V|v][.|/|:]? [\'|\"]?([A-Za-z0-9]+[ ]?[A-Za-z0-9]*)[\'|\"]?'
    all_patterns = '|'.join([pattern1, pattern2, pattern3, extra_MV_patterns])
    vessels_c = re.compile(r'%s'%all_patterns)
    return vessels_repository, vessels_c

if __name__ == "__main__":
    print("Start principle net...")
    print(README)

    #Config:
    DEBUG = True
    DEBUG = False
    DATA_PATH_PREFIX = './data/data_bonding_net/'
    msg_files = glob.glob(DATA_PATH_PREFIX+"/msgs/*.msg")

    #Repos:
    couterparts_repository = get_counterparts_repository()
    vessels_repository, vessels_c = get_vessels_repository()

    #Loop over msgs:
    num_of_failures = 0
    failure_list = []
    for this_msg_file in tqdm.tqdm(msg_files):
        if DEBUG:
            print("\nIn file %s: "%this_msg_file)
        try:
            msg, msg_content = parse_msg(this_msg_file)
        except:
            num_of_failures += 1
            failure_list.append(this_msg_file)
            continue
        sender_email = get_sender_email(msg)
        if judge_if_direct_counterpart(sender_email) is True:
            vessels_name = retrieve_vessel(msg_content, vessels_c)
            skypes_id = retrieve_skype(msg_content)
            blob = parse_blob(vessels_name, sender_email, skypes_id)
        else:
            print("Not direct couterpart: %s"%sender_email)

    print("Failures %s:"%num_of_failures, failure_list)





