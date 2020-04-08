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
import argparse
import shutil
try:
    from multiprocessing.pool import Pool
    from multiprocessing import Manager
    from multiprocessing import cpu_count
    MP = True
    print("Running parallelism... Your PC has %s cpu core"%cpu_count())
except Exception as e:
    print(e, "Running serial...")
    MP = False

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
Msn/Skype:Fengncl@hotmail.com
Skype/MSn:Fengncl@hotmail.com
'''

VESSELS_TESTER = \
'''
For MV Huayang
MV CORAL GEM
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
            for i in re.findall(r'[ ]*[0-9\.-][ ]*', vessel, re.I):  #name like huayang -1 will be additionally added as huyang-1 as well
                add_vessel = vessel.replace(i, i.strip(' '))
                safe_name.append(add_vessel)
        else:
            unsafe_name.append(vessel)
    safe_name.sort(key = lambda i:len(i),reverse=True)
    unsafe_name.sort(key = lambda i:len(i),reverse=True)
    #Use raw name for safe:
    head = r'[\n\r\t]*('
    tail = ')[^A-Za-z0-9]'
    pattern1 = head+'|'.join(safe_name)+tail
    #Mv m.v m/v added for unsafe:
    head = r'[M][\.|/]?[V][\.|/:]? [\'|\"]?('
    tail = ')[^A-Za-z0-9]'
    pattern2 = head+'|'.join(unsafe_name)+tail
    #Porpose propse purpose pps ppse offer for:
    head = r'[p][u|r]?[r|o]*[p][o]?[s][e]?|offer for[ :\n\r\t]*[\'|\"]?('
    tail = ')[^A-Za-z0-9]'
    pattern3 = head+'|'.join(unsafe_name)+tail
    #Not in repository:  mv
    #extra_MV_patterns = '[M|m][.|/]?[V|v][.|/|:]? [\'|\"]?([A-Za-z0-9]+[ |\.]?[A-Za-z0-9]*)[\'|\"]?'
    #all_patterns = '|'.join([pattern1, pattern3])
    all_safe_patterns = '|'.join([pattern1,'(TOKEN-FUSS-TOKEN-FUSS)'])
    all_unsafe_patterns = '|'.join([pattern3, pattern2])
    vessels_patterns = {}
    vessels_patterns['safe'] = re.compile(r'%s'%all_safe_patterns, re.I)
    vessels_patterns['unsafe'] = re.compile(r'%s'%all_unsafe_patterns, re.I)
    return vessels_repository, vessels_patterns


def parse_msg(msg_file_path):
    f = msg_file_path  # Replace with yours
    try:
        msg = extract_msg.Message(f)
        msg_sender = msg.sender
        msg_subject = msg.subject
        msg_body = msg.body
        msg_content = msg_subject + msg_body  #Content include all
    except Exception as e:
        if DEBUG:print("Failed on extract_msg!",e)
        return False, False, False
    for i in re.findall(r'( <mailto:[\.A-Za-z0-9\-_@:%]*> )', msg_content):
        msg_content = msg_content.replace(i, '')
    msg_content = msg_content.replace(' @', '@')
    msg_content = msg_content.replace('@ ', '@')
    if DEBUG:
        print("In file %s: "%f)
        #print("msg_subj:", msg_content)
        #print(msg_content[:50])
    return msg_sender, msg_subject, msg_content

def retrieve_sender_email(msg_sender):
    sender_email_raw = msg_sender
    if sender_email_raw is None:
        sender_email_raw = ' '
    pattern = re.compile('<(.*)>')
    sender_email = pattern.findall(sender_email_raw)
    if DEBUG:
        print("Got sender:")
        print(sender_email)
    if len(sender_email)>0:
        sender_email = sender_email[0].lower()
    else:
        if DEBUG:print("Error getting sender: %s"%sender_email_raw)
        sender_email = ""
    return sender_email

def judge_if_is_not_REply_or_others(msg_subject, msg_subject_and_content):
    other_trashes_in_subject_and_content = '''
            failure
            rejected
            退信
            returned
            Recapito ritardato
            Mailer-Daemon@mail4.bancosta.it
            Delivery failure
            Delivery delayed
            Undeliverable
            Non recapitabile
            Systems bounce
            support@tnticker.com
            This email address is no longer in use
            '''.replace(' ','').split('\n')
    other_trashes_in_subject_and_content = list(set(other_trashes_in_subject_and_content)-set({''}))
    other_trashes_pattern = re.compile('|'.join(other_trashes_in_subject_and_content), re.I)
    if len(re.findall('r[e]?[ply]?:', msg_subject, re.I))>0:    #If this is REply!! may cotian many irrelevant ships, so No!
        return False
    elif len(other_trashes_pattern.findall(msg_subject_and_content))>0:   #If others
        if DEBUG:print("Other trashes found:%s"%other_trashes_pattern.findall(msg_subject_and_content))
        return False
    else:
        return True

def judge_if_direct_counterpart(sender_email, counterparts_repository):
    #i, the keyword of counterparts name.
    tmp = np.array([len(re.findall(i, sender_email, re.I)) for i in counterparts_repository])
    if tmp.sum()==1:
        direct_counterpart = True
    elif tmp.sum()>1:
        print("Warning multiple key words in sender address:", np.array(counterparts_repository)[np.where(tmp!=0)[0].reshape(-1,1)].tolist(), 'in', sender_email)
        direct_counterpart = True
    else:
        direct_counterpart = False
    return direct_counterpart

def retrieve_vessel(msg_content, vessels_patterns):
    vessels_name = []
    vessels_pattern_safe = vessels_patterns['safe']
    vessels_pattern_unsafe = vessels_patterns['unsafe']
    vessels_name_raw_safe = vessels_pattern_safe.findall(msg_content)   #return []
    vessels_name_raw_unsafe = vessels_pattern_unsafe.findall(msg_content)  #return [()]
    #If safe name occured then ignore unsafe name:
    redudant_name = []
    for temp in vessels_name_raw_unsafe:
        for short_name in temp:
            if short_name in str(vessels_name_raw_safe) and short_name not in redudant_name:
                redudant_name.append(short_name)
    #Taken out names:
    vessels_name_raw = vessels_name_raw_safe + vessels_name_raw_unsafe
    if len(vessels_name_raw) == 0:
        pass
    else:
        for i in vessels_name_raw:
            for j in i:
                if j != '' and j not in redudant_name:
                    vessels_name.append(j.strip(' '))
    order = vessels_name.index
    vessels_name = list(set(vessels_name))
    vessels_name.sort(key=order)
    for idx,this_vessel_name in enumerate(vessels_name):
        vessels_name[idx] = this_vessel_name.upper()
    if DEBUG:
        print("Got vessels name:")
        print(vessels_name)
    #TODO: To fix MV CORAL and MV CORAL GEM     FIXED 04 01
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
    patterns.append('msn/skype[ ]*[:]?[ ]*([ 0-9a-z_\-:\.@]+)')
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
    while '' in skypes_id:skypes_id.remove('')
    for idx,this_skype_id in enumerate(skypes_id):
        skypes_id[idx] = this_skype_id.lower()
    if DEBUG:
        print("Got skype:")
        print(skypes_id)
    return skypes_id

def retrieve_pic_mailboxes(msg_content, sender_email):
    pattern = re.compile('([a-z0-9_\.]+@[a-z0-9\.]+\.[a-z]+)', re.I)
    pic_mailboxes  = pattern.findall(msg_content)
    pic_mailboxes += [sender_email]
    if DEBUG:
        print("Got mailboxes:")
        print(pic_mailboxes)
    for idx,this_pic_mailbox in enumerate(pic_mailboxes):
        pic_mailboxes[idx] = this_pic_mailbox.lower()
    return pic_mailboxes

def retrieve_pic_skype(msg_content, vessels_name, skypes_id):
    pic_skype = None
    if len(vessels_name)==1 and len(skypes_id)==1:
        if DEBUG:print('Got A pic skype for %s %s'%(vessels_name, skypes_id))
        pic_skype = skypes_id[0]
    return pic_skype

def parse_blob(vessels_name, sender_email, skypes_id, pic_mailboxes, pic_skype):
    blob = {'MV':[], 'SENDER':sender_email, 'SKYPES':[], 'MAILBOXES':[]}
    if (len(vessels_name)+len(skypes_id))==0:
        pass
    else:
        blob['MV'] = vessels_name
        blob['SKYPES'] = skypes_id
        blob['MAILBOXES'] = pic_mailboxes
        blob['PIC_SKYPE'] = pic_skype
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

def solve_one_msg(struct):
    global MV_SENDER_BLOB, SENDER_MAILBOXES_BLOB, SENDER_SKYPES_BLOB, MV_SKYPE_BLOB, TRASH_SENDER
    this_msg_file, num_of_failures, failure_list = struct[0], struct[1], struct[2]
    if DEBUG:print(this_msg_file)
    msg_sender, msg_subject, msg_content = parse_msg(this_msg_file)
    if msg_subject == False:
        num_of_failures += 1
        failure_list.append(this_msg_file)
        #return MV_SENDER_BLOB, SENDER_MAILBOXES_BLOB, SENDER_SKYPES_BLOB, MV_SKYPE_BLOB, TRASH_SENDER
        return
    sender_email = retrieve_sender_email(msg_sender)
    if judge_if_is_not_REply_or_others(msg_subject, msg_content) is True and len(sender_email)>0:
        if judge_if_direct_counterpart(sender_email, counterparts_repository) is True:
            vessels_name = retrieve_vessel(msg_content, vessels_patterns)
            skypes_id = retrieve_skype(msg_content)
            pic_mailboxes = retrieve_pic_mailboxes(msg_content, sender_email)
            pic_skype = retrieve_pic_skype(msg_content, vessels_name, skypes_id)
            blob = parse_blob(vessels_name, sender_email, skypes_id, pic_mailboxes, pic_skype)
            #PARSE ShARED CORE BLOB,
            #SUBSTUTIVE:
            for mv in blob['MV']:
                MV_SENDER_BLOB[mv] = blob['SENDER'] 
            for mv in blob['MV']:
                if blob['PIC_SKYPE'] is None and mv in MV_SKYPE_BLOB.keys():  
                    pass   #Leave it along, it already has pic, and it's now None, so skip.
                else:
                   MV_SKYPE_BLOB[mv] = blob['PIC_SKYPE'] #Update pic only when actually found and is not None
            #ACCUMULATIVE:
            sender = blob['SENDER']
            #embed()
            if sender in SENDER_MAILBOXES_BLOB.keys():
                SENDER_MAILBOXES_BLOB[sender] += blob['MAILBOXES']
                SENDER_MAILBOXES_BLOB[sender] = list(set(SENDER_MAILBOXES_BLOB[sender]))
            else:
                SENDER_MAILBOXES_BLOB[sender] = blob['MAILBOXES']
            if sender in SENDER_SKYPES_BLOB.keys():
                SENDER_SKYPES_BLOB[sender] += blob['SKYPES']
                SENDER_SKYPES_BLOB[sender] = list(set(SENDER_SKYPES_BLOB[sender]))
            else:
                SENDER_SKYPES_BLOB[sender] = blob['SKYPES']
        else:
            TRASH_SENDER.append(sender_email)
            if DEBUG:
                print("Not direct couterpart: %s"%sender_email)
    else:
        if DEBUG:
            print("This is Reply! pass")
        pass
    return

if __name__ == "__main__":
    print("Start principle net...")

    #Config:
    parser = argparse.ArgumentParser()
    parser.add_argument("-D", '--DEBUG', action='store_true', default=False)
    parser.add_argument("-S", '--FROM_SCRATCH', action='store_true', default=False)
    args = parser.parse_args()
    FROM_SCRATCH = args.FROM_SCRATCH
    DEBUG = args.DEBUG
    DATA_PATH_PREFIX = './data/data_bonding_net/'
    BLACKLIST_MAILBOXES = [i.strip('\n').lower() for i in open(DATA_PATH_PREFIX+"/pic_blacklist.txt").readlines()]
    TEST = ''
    #From scratch? Checkpoint?
    #global MV_SENDER_BLOB, SENDER_MAILBOXES_BLOB, SENDER_SKYPES_BLOB, MV_SKYPE_BLOB, TRASH_SENDER
    if not MP:
        MV_SENDER_BLOB = {}
        SENDER_MAILBOXES_BLOB = {}
        SENDER_SKYPES_BLOB = {}
        MV_SKYPE_BLOB = {}
        TRASH_SENDER = []
    else:   #MP    
        manager = Manager()
        MV_SENDER_BLOB = manager.dict()
        SENDER_MAILBOXES_BLOB = manager.dict()
        SENDER_SKYPES_BLOB = manager.dict()
        MV_SKYPE_BLOB = manager.dict()
        TRASH_SENDER = manager.list()

    if FROM_SCRATCH:
        print("Run from scratch, moving msgs to work place...")
        #os.system("mv %s/msgs/%s/consumed/*.msg %s/msgs/%s/"%(DATA_PATH_PREFIX, TEST, DATA_PATH_PREFIX, TEST))
        consumed_files = glob.glob("%s/msgs/%s/consumed/*.msg"%(DATA_PATH_PREFIX, TEST))
        for this_consumed_file in consumed_files:
            shutil.move(this_consumed_file, this_consumed_file.replace("consumed",""))
        #checkpoint = pd.DataFrame()
    else:
        print("Running restart.... Loading checkpoint...")
        try:
            restart_MV_SENDER = pd.read_csv("output/core_MV_SENDER.csv")
            restart_SENDER_MAILBOXES = pd.read_csv("output/core_SENDER_MAILBOXES.csv")
            restart_SENDER_SKYPES = pd.read_csv("output/core_SENDER_SKYPES.csv")
            restart_MV_SKYPE = pd.read_csv("output/core_MV_SKYPE.csv")
            TRASH_SENDER = list(pd.read_csv('output/TRASH_SENDER.csv')['TRASH_SENDER'])
        except Exception as e:
            print("e\n Error reading restart file, you may want to run from scratch? with -S")
            sys.exit()
        for i in restart_MV_SENDER.iterrows():
            MV_SENDER_BLOB[i[1].MV] = i[1].SENDER
        for i in restart_SENDER_MAILBOXES.iterrows():
            tmp = i[1].MAILBOXES.strip("[]'").split("', '")
            try:
                tmp.remove('')
            except:
                pass
            SENDER_MAILBOXES_BLOB[i[1].SENDER] = tmp
        for i in restart_SENDER_SKYPES.iterrows():
            tmp = i[1].SKYPES.strip("[]'").split("', '")
            try:
                tmp.remove('')
            except:
                pass
            SENDER_SKYPES_BLOB[i[1].SENDER] = tmp
        for i in restart_MV_SKYPE.iterrows():
            if i[1].PIC_SKYPE is not np.nan:
                tmp = i[1].PIC_SKYPE.strip("[]'").split("', '")
            else:
                tmp = ''
            try:
                tmp.remove('')
            except:
                pass
            MV_SKYPE_BLOB[i[1].MV] = tmp
    #Will work on files:
    msg_files = glob.glob(DATA_PATH_PREFIX+"/msgs/%s/*.msg"%TEST)
    msg_files = msg_files[:]
    #Repos:
    counterparts_repository = get_counterparts_repository()
    vessels_repository, vessels_patterns = get_vessels_repository_and_patterns()
    #embed()

    #Loop over msgs:
    num_of_failures = 0
    failure_list = []
    if MP:
        pool = Pool(processes=cpu_count()*2)
        struct_list = []
        for i in range(len(msg_files)):
            struct_list.append([msg_files[i], num_of_failures, failure_list])
        rs = pool.map_async(solve_one_msg, struct_list)
        while (True):
          if (rs.ready()): break
          remaining = rs._number_left
          print("Waiting for", remaining, "tasks to complete...")
          time.sleep(0.5)
    else:
        for this_msg_file in tqdm.tqdm(msg_files):
            struct_this = [this_msg_file, num_of_failures, failure_list]
            #MV_SENDER_BLOB, SENDER_MAILBOXES_BLOB, SENDER_SKYPES_BLOB, MV_SKYPE_BLOB, TRASH_SENDER = solve_one_msg(struct_this)
            solve_one_msg(struct_this)

    #for Mailboxes, Blacklist and -skype:
    for sender in SENDER_MAILBOXES_BLOB.keys():
        SENDER_MAILBOXES_BLOB[sender] = list(set(SENDER_MAILBOXES_BLOB[sender])-set(SENDER_SKYPES_BLOB[sender])-set(BLACKLIST_MAILBOXES))
    print("Failures %s:"%num_of_failures)

    #embed()
    #Generate outputs:
    output_MV_SENDER = pd.DataFrame({'MV':list(MV_SENDER_BLOB.keys()), 'SENDER':list(MV_SENDER_BLOB.values())})
    output_SENDER_MAILBOXES = pd.DataFrame({'SENDER':list(SENDER_MAILBOXES_BLOB.keys()), 'MAILBOXES':list(SENDER_MAILBOXES_BLOB.values())}) 
    output_SENDER_SKYPES = pd.DataFrame({'SENDER':list(SENDER_SKYPES_BLOB.keys()), 'SKYPES':list(SENDER_SKYPES_BLOB.values())})
    output_MV_SKYPE = pd.DataFrame({'MV':list(MV_SKYPE_BLOB.keys()), 'PIC_SKYPE':list(MV_SKYPE_BLOB.values())})
    output_MV_SENDER_MAILBOXES_SKYPE = pd.DataFrame({'MV':list(MV_SENDER_BLOB.keys()), 'SENDER':list(MV_SENDER_BLOB.values()), 'MAILBOXES':list(map(SENDER_MAILBOXES_BLOB.get, MV_SENDER_BLOB.values())), 'SKYPES':list(map(SENDER_SKYPES_BLOB.get, MV_SENDER_BLOB.values())), 'PIC_SKYPE':list(MV_SKYPE_BLOB.values())})
    TRASH_SENDER = pd.DataFrame({'TRASH_SENDER': list(TRASH_SENDER)})
    ok = pd.DataFrame({ 'ok': list(SENDER_MAILBOXES_BLOB.keys())})
    try:
        output_MV_SENDER.to_csv('output/core_MV_SENDER.csv', index=False)   #TODO: Too large will cause error??
        output_SENDER_MAILBOXES.to_csv('output/core_SENDER_MAILBOXES.csv', index=False)
        output_SENDER_SKYPES.to_csv('output/core_SENDER_SKYPES.csv', index=False)
        output_MV_SKYPE.to_csv('output/core_MV_SKYPE.csv', index=False)
        TRASH_SENDER.to_csv('output/TRASH_SENDER.csv', index=False)
        ok.to_csv('output/OK.csv', index=False)
        output_MV_SENDER_MAILBOXES_SKYPE.to_csv('output/core_MV_SENDER_MAILBOXES_SKYPE.csv', index=False)
    except:
        print("Saving to csv error, your computer may be A piece of Shit! Will now interact mannually!")
        embed()

    #To Consumed:
    print("Moving to consumed...")
    for msg_file in msg_files:
        tmp = msg_file.split('/')
        tmp.insert(-1, 'consumed')
        tmp = '/'.join(tmp)
        os.rename(msg_file, tmp)
        pass
    #embed()


    print("All finished.")
