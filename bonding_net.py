import os,sys,time
import pandas as pd
from IPython import embed
import re
import glob
import tqdm

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
    msg_content = ''.join(open(msg_file_path, 'r').readlines()).strip('\n')
    print("Msg read:", "~"*24)
    print(msg_content)
    print("~"*30)
    return msg_content

def get_sender_email(msg_content):
    sender_email = None
    c = re.compile(r'[0-9a-z_-]+@([0-9a-z]+.)+[a-z]+', re.I)
    s = c.search(msg_content)
    if s:
        sender_email = s.group()
        print(sender_email)
    return sender_email

def judge_if_direct_counterpart(sender_email):
    direct_counterpart = True
    return direct_counterpart

def retrieve_vessel(msg_content):
    vessels_repository = open(DATA_PATH_PREFIX+"/vessels_repository.txt", 'r').readlines()
    vessels_name = []
    print("Got vessels name:", vessels_name)
    return vessels_name

def retrieve_skype(msg_content):
    skypes_id = None
    c = re.compile(r'skype[ 0-9a-z_\-:]+', re.I)
    s = c.search(msg_content)
    if s:
        skypes_id = s.group()
        print(skypes_id)
    return skypes_id

def parse_blob(vessels_name, sender_email, skypes_id):
    blob = []
    print("Got blob:", blob)
    return blob

if __name__ == "__main__":
    print("Start principle net...")
    print(README)

    #Config:
    DATA_PATH_PREFIX = './data/data_principle_net/'
    msg_files = glob.glob(DATA_PATH_PREFIX+"/msgs/*.msg")

    #Loop over msgs:
    for this_msg_file in msg_files:
        print("\nIn file %s: "%this_msg_file)
        msg_content = parse_msg(this_msg_file)
        sender_email = get_sender_email(msg_content)
        if judge_if_direct_counterpart(sender_email) is True:
            vessels_name = retrieve_vessel(msg_content)
            skypes_id = retrieve_skype(msg_content)
            blob = parse_blob(vessels_name, sender_email, skypes_id)
        else:
            print("Not direct couterpart: %s"%sender_email)






