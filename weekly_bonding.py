import os,sys,time
import pandas as pd
from IPython import embed
import re
import glob
import tqdm
import extract_msg
#import openpyxl
import xlrd
import yaml
from collections import Counter
import numpy as np

SKYPE_TESTER = \
'''
SKYPE: alloisi@olorenzo
Skype: Tassos Armyriotis
Skype: live:Tassriotis
skype : izden.unlu1
skype : izden_unlu221
skype id: cvnrpgh
(SKYPE) live:wynn_mi11ttpe
(SKYPEid) live:wynn_mi22ttpe
SKYPEid) live:wynn_mitt33pe
SKYPE) live:wynn_mittp444e
skype live:sychioi
skype:impsmb<abc>
sta1rvo(skype id)
star22vo(skype)
Skype: + Filippo.Gabutto
SKYPE (live) mathewmohanchat
'''

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
def get_vessels_repository_and_patterns():
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
    #extra_MV_patterns = '[M|m][.|/]?[V|v][.|/|:]? [\'|\"]?([A-Za-z0-9]+[ |\.]?[A-Za-z0-9]*)[\'|\"]?'
    all_patterns = '|'.join([pattern1, pattern2, pattern3])
    vessels_pattern = re.compile(r'%s'%all_patterns)
    return vessels_repository, vessels_pattern


def parse_msg(msg_file_path):
    f = msg_file_path  # Replace with yours
    msg = extract_msg.Message(f)
    #msg_sender = msg.sender
    #msg_date = msg.date
    msg_subj = msg.subject
    msg_content = msg_subj + msg.body
    if DEBUG:
        print("Got msg:\n", "~"*24)
        #print(msg_content[:50])
        print("~"*30)
    return msg, msg_content

def get_sender_email(msg):
    sender_email_raw = msg.sender
    pattern = re.compile('<(.*)>')
    sender_email  = pattern.findall(sender_email_raw)
    if DEBUG:
        print("Got sender:")
        print(sender_email)
    return sender_email

def judge_if_direct_counterpart_and_not_REply(msg, sender_email, counterparts_repository):
    #i, the keyword of counterparts name.
    tmp = np.array([len(re.findall(i, sender_email[0], re.I)) for i in counterparts_repository])
    if len(re.findall('re:', msg.subject, re.I))>0:    #If this is REply!! may cotian many irrelevant ships, so No!
        return False
    if tmp.sum()==1:
        direct_counterpart = True
    elif tmp.sum()>1:
        print("Warning multiple:", np.array(counterparts_repository)[np.where(tmp!=0)[0].reshape(-1,1)].tolist(), 'in', sender_email)
        direct_counterpart = True
    else:
        direct_counterpart = False
    return direct_counterpart

def retrieve_vessel(msg_content, vessels_pattern):
    vessels_name = []
    vessels_name_raw = vessels_pattern.findall(msg_content)
    if len(vessels_name_raw) == 0:
        pass
    else:
        for i in vessels_name_raw:
            for j in i:
                if j != '':
                    vessels_name.append(j.strip(' '))
    order = vessels_name.index
    vessels_name = list(set(vessels_name))
    vessels_name.sort(key=order)
    if DEBUG:
        print("Got vessels name:")
        print(vessels_name)
    return vessels_name

def retrieve_skype(msg_content):
    skypes_id = []
    #tmp = re.compile('skype', re.I)
    #tt = tmp.search(msg_content)
    #if tt:
    #    print(msg_content[tt.span()[0]-min(25,tt.span()[0]-1):tt.span()[1]+25])
    patterns = []
    #skype id : xxx
    patterns.append('skype \(live\)[ ]?[:]?[ ]*([ 0-9a-z_\-:\.@]+)')
    patterns.append('skype/msn[ ]*[:]?[ ]*([ 0-9a-z_\-:\.@]+)')
    patterns.append('skypeid[ ]?[:]?[ ]*([ 0-9a-z_\-:\.@]+)')
    patterns.append('skype id[ ]?[:]?[ ]*([ 0-9a-z_\-:\.@]+)')
    patterns.append('\(skypeid\)[ ]*[:]?[ ]*([ 0-9a-z_\-:\.@]+)')
    patterns.append('\(skype id\)[ ]*[:]?[ ]*([ 0-9a-z_\-:\.@]+)')
    patterns.append('skype id\)[ ]*[:]?[ ]*([ 0-9a-z_\-:\.@]+)')
    patterns.append('skypeid\)[ ]*[:]?[ ]*([ 0-9a-z_\-:\.@]+)')
    patterns.append('skype[ ]*[:]?[]*\+[ ]?([ 0-9a-z_\-:\.@]+)')
    patterns.append('\(skype\)[ ]*[:]?[ ]*([ 0-9a-z_\-:\.@]+)')
    patterns.append('([0-9a-z_\-:\.@]+)\(skypeid\)')
    patterns.append('([0-9a-z_\-:\.@]+)\(skype id\)')
    patterns.append('([0-9a-z_\-:\.@]+)\(skype\)')
    patterns.append('skype\)[ ]*[:]?[ ]*([ 0-9a-z_\-:\.@]+)')
    #Skype : xxxx
    patterns.append('skype[ ]*[:]?[ ]*([ 0-9a-z_\-:\.@]+)')
    all_patterns = '|'.join(patterns)
    c = re.compile(all_patterns, re.I)
    skypes_id_raw = c.findall(msg_content)
    skypes_id = []
    if len(skypes_id_raw) == 0:
        pass
    else:
        for i in skypes_id_raw:
            for j in i:
                if j != '':
                    skypes_id.append(j.strip(' ').strip(':'))
    order = skypes_id.index
    skypes_id = list(set(skypes_id))
    skypes_id.sort(key=order)
    if DEBUG:
        print("Got skype:")
        print(skypes_id)
    while '' in skypes_id:skypes_id.remove('')
    return skypes_id

def retrieve_pic_mailboxes(msg_content):
    pattern = re.compile('([A-Za-z0-9\.]+@[A-Za-z0-9\.]+)')
    pic_mailboxes  = pattern.findall(msg_content)
    if DEBUG:
        print("Got PICmailboxes:")
        print(pic_mailboxes)
    return pic_mailboxes


def parse_blob(vessels_name, sender_email, skypes_id, pic_mailboxes):
    blob = {'MV':[], 'SENDER':sender_email, 'SKYPE':[], 'PIC':[]}
    if (len(vessels_name)+len(skypes_id))==0:
        pass
    else:
        blob['MV'] = vessels_name
        blob['SKYPE'] = skypes_id
        blob['PIC'] = pic_mailboxes
        #print(yaml.dump(blob))
    return blob

def get_counterparts_repository():
    counterparts_repository = open(DATA_PATH_PREFIX+"/counterparts_repository.txt", 'r').readlines()
    counterparts_repository = ''.join(counterparts_repository).split('\n')
    try:
        counterparts_repository.remove('')
    except:
        pass
    return counterparts_repository


if __name__ == "__main__":
    print("Start principle net...")
    print(README)

    #Config:
    DEBUG = True
    DEBUG = False
    DATA_PATH_PREFIX = './data/data_bonding_net/'
    msg_files = glob.glob(DATA_PATH_PREFIX+"/msgs/*.msg")
    #Repos:
    counterparts_repository = get_counterparts_repository()
    vessels_repository, vessels_pattern = get_vessels_repository_and_patterns()

    #Loop over msgs:
    num_of_failures = 0
    failure_list = []
    MV_SENDER_BLOB = {}
    SENDER_PIC_BLOB = {}
    trash_sender = []
    for this_msg_file in tqdm.tqdm(msg_files[:]):
    #for this_msg_file in msg_files:
        if DEBUG:
            print("\nIn file %s: "%this_msg_file)
        try:
            msg, msg_content = parse_msg(this_msg_file)
        except Exception as e:
            num_of_failures += 1
            failure_list.append(this_msg_file)
            if DEBUG:print(this_msg_file, e)
            continue
        sender_email = get_sender_email(msg)
        if judge_if_direct_counterpart_and_not_REply(msg, sender_email, counterparts_repository) is True:
            vessels_name = retrieve_vessel(msg_content, vessels_pattern)
            skypes_id = retrieve_skype(msg_content)
            pic_mailboxes = retrieve_pic_mailboxes(msg_content)
            blob = parse_blob(vessels_name, sender_email, skypes_id, pic_mailboxes)
            #PARSE BONGINDG BLOB:
            for mv in blob['MV']:
                if mv in MV_SENDER_BLOB.keys():
                    MV_SENDER_BLOB[mv] += blob['SENDER']
                else:
                    MV_SENDER_BLOB[mv] = blob['SENDER']
            for sender in blob['SENDER']:
                if sender in SENDER_PIC_BLOB.keys():
                    SENDER_PIC_BLOB[sender] += blob['PIC']
                else:
                    SENDER_PIC_BLOB[sender] = blob['PIC']
        else:
            trash_sender.append(sender_email[0])
            if DEBUG:
                print("Not direct couterpart: %s"%sender_email)

    #Sets over BLOB:
    for mv in MV_SENDER_BLOB.keys():
        MV_SENDER_BLOB[mv] = MV_SENDER_BLOB[mv][0]
    for sender in SENDER_PIC_BLOB.keys():
        SENDER_PIC_BLOB[sender] = list(set(SENDER_PIC_BLOB[sender]))

    print("Failures %s:"%num_of_failures, failure_list)


    #Get outputs:
    data_MV_SENDER = pd.DataFrame({'MV':list(MV_SENDER_BLOB.keys()), 'SENDER':list(MV_SENDER_BLOB.values())})
    data_SENDER_PIC = pd.DataFrame({'SENDER':list(SENDER_PIC_BLOB.keys()), 'PIC':list(SENDER_PIC_BLOB.values())})
    data_MV_SENDER_PIC = pd.DataFrame({'MV':list(MV_SENDER_BLOB.keys()), 'SENDER':list(MV_SENDER_BLOB.values()), 'PIC':list(map(SENDER_PIC_BLOB.get, MV_SENDER_BLOB.values()))})
    trash_sender = pd.DataFrame({'trash_sender': trash_sender})
    ok = pd.DataFrame({ 'ok': list(SENDER_PIC_BLOB.keys())})
    data_MV_SENDER.to_csv('output/core_MV_SENDER.csv', index=False)
    data_SENDER_PIC.to_csv('output/core_SENDER_PIC.csv', index=False)
    trash_sender.to_csv('output/trash_sender.csv', index=False)
    ok.to_csv('output/ok.csv', index=False)
    data_MV_SENDER_PIC.to_csv('output/core_MV_SENDER_PIC.csv', index=False)

