import os,sys,time
import pandas as pd
from IPython import embed
import re
import glob
import tqdm
from extract_msg import Message
import textract
import olefile
import xlrd
import yaml
from collections import Counter
import numpy as np
import argparse
import shutil
from time_counter import calc_time
import pickle

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
SKYPE: alloisi@olorenzo1
Skype: Tassos Armyriotis2
Skype: live:Tassriotis3
skype : izden.unlu4
skype id: cvnrpgh5
(SKYPE) live:wynn_mi11ttpe6
(SKYPEid) live:wynn_mi22ttpe7
SKYPEid) live:wynn_mitt33pe8
SKYPE) live:wynn_mittp444e9
skype live:sychioi10
skype:impsmb<abc>11
sta1rvo(skype id)12
star22vo(skype)13
Skype: + Filippo.Gabutto14
SKYPE (live) mathewmohanchat15
Msn/Skype:Fengncl@hotmail.com16
Skype/MSn:Fengncl@hotmail.com17
MSN/SKYPE: Feng123@hotmail.com18
'''

VESSELS_TESTER = \
'''
CORAL GEM1
For MV Huayang2
MV CORALGEM3
MV CORAL4
'''

PERSON_TESTER = \
'''
M.V. IKAN PANDAN-(58DWT)-Open Vancouver 20-30 Jan- Elaine
MV. IKAN PULAS- (63DWT)-Open Corpus Christi 21 Jan-Beatriz
HEathiz:MV. GLORIOUS FUJI(250dwt)
HEathiz2-MV. GLORIOUS FUJI(250dwt)
'''

README = \
'''
**************************
*整体逻辑*
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
@calc_time
def get_vessels_repository_and_patterns(args):
    workbook = xlrd.open_workbook(DATA_PATH_PREFIX+"/vessels_repository.xlsx")
    vessels_repository_raw = []
    for sheet in workbook.sheet_names():
        table = workbook.sheet_by_name(sheet)
        vessels_repository_raw += table.col_values(1)[1:]
    vessels_repository = list(set(vessels_repository_raw))
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
    head = r'[M][\.|/]?[V][\.|/:]?[\'|\"]?('
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

@calc_time
def get_person_name_repository_and_patterns(args):
    def names_enrichment(old_names):
        enriched_names = []
        for name in old_names:              #Alpha Beta
            if len(name.split(' '))==2:
                alpha = name.split(' ')[0]
                beta = name.split(' ')[1]
                enriched_names += [alpha] if len(alpha)>1 else [] #Alpha
                enriched_names += [beta] if len(beta)>1 else [] #Beta
                enriched_names += [alpha + ' ' + beta[0]]    #Alpha B
                enriched_names += [alpha[0] + beta[0]]    #AB
            if len(name.split(' '))==3:  #Alpha Beta Gama
                alpha = name.split(' ')[0]
                beta = name.split(' ')[1]
                gama = name.split(' ')[2]
                enriched_names += [alpha] if len(alpha)>1 else [] #Alpha
                enriched_names += [gama] if len(gama)>1 else [] #Gama
                enriched_names += [alpha + ' ' + gama]  #Alpha Gama
                enriched_names += [alpha + ' ' + gama[0]]  #Alpha G
                enriched_names += [alpha + ' ' + beta[0] + ' ' + gama]  #Alpha B Gama
                enriched_names += [alpha[0] + beta[0] + gama[0]]  #ABG
                enriched_names += [alpha[0] + gama[0]]  #AG
                enriched_names += [alpha[0] + beta[0] + gama]  #AB Gama
        enriched_names += old_names
        for idx,i in enumerate(enriched_names):
            enriched_names[idx] = i.upper()
        enriched_names = list(set(enriched_names))
        return enriched_names
    Quick_path = "data/data_bonding_net/company_info.pkl"
    if args.QUICK_COMPANY_DATA:
        [company_person_skype, company_person_skype_pattern] = pickle.load(open(Quick_path, 'rb'))
    else:    #Not quick data
        data = pd.read_excel(DATA_PATH_PREFIX+"/person_names_repository.xls", index_col = 'bonding')
        #Reading its content mannualy......
        company_person_skype = {}
        company_person_skype_pattern = {}
        for this_company in data.index:
            company_person_skype[this_company] = {}
            for pic in data.loc[this_company].columns:
                names = data.loc[this_company, pic].iloc[0]
                skype = data.loc[this_company, pic].iloc[1]
                if not isinstance(names, str):continue
                names = names.split(',')
                names = names_enrichment(names)
                for name in names:    #names are:    ao,Anders Ostang
                    assert isinstance(name, str)
                    company_person_skype[this_company][name] = skype
                    company_person_skype_pattern[this_company] = re.compile("[^A-Z0-9]("+'|'.join(names)+")[^A-Z0-9]", re.I)
            print("Built person name relations for this_company...", this_company, company_person_skype[this_company])
        pickle.dump([company_person_skype,company_person_skype_pattern], open(Quick_path, 'wb'))
    return company_person_skype, company_person_skype_pattern

@calc_time
def parse_msg(args, msg_file_path):
    def olefile_read(msg_file_path):
        msg = olefile.OleFileIO(msg_file_path)
        b = msg.exists('__substg1.0_1000001E')
        with msg.openstream('__substg1.0_1000001E' if b else '__substg1.0_1000001F') as f:
            msg_body = f.read()
        msg.close()
        return msg_body.decode()   #TODO: 这里是否有回复？？ 此时回复是否有法判断？
    f = msg_file_path  # Replace with yours
    msg_sender = None
    try:
        msg = Message(f)
        try:
            msg_sender = msg.sender
        except:
            msg_sender = 'Cant get sender'
        try:
            msg_subject = msg.subject
        except:
            msg_subject = 'Cant get subject'
        try:
            msg_body = msg.body
        except:
            msg_body = 'Cant get body'
        if msg_sender is None:
            msg_sender = 'Cant get sender'
        if msg_subject is None:
            msg_subject = 'Cant get subject'
        if msg_body is None:
            msg_body = 'Cant get body'
    except Exception as e:
        try:
            try:
                msg_body = olefile_read(f)
            except:
                msg_body = textract.parsers.process(f)
                print("*"*99)
            msg_sender = 'Wont have sender' if msg_sender is None else msg_sender
            msg_subject = 'Wont have subject' if msg_subject is None else msg_subject
        except:
            if args.DEBUG:print("Failed on extract_msg!",e)
            return msg_sender, False, False
    msg_content = msg_subject + msg_body  #Content include all
    for i in re.findall(r'([ ]?<mailto:[\.A-Za-z0-9\-_@:%]*>[ ]?)', msg_content):
        msg_content = msg_content.replace(i, '')
    msg_content = msg_content.replace(' @', '@')
    msg_content = msg_content.replace('@ ', '@')
    msg_content = msg_content.replace('live: ', 'live:')
    if args.DEBUG:print("In file %s: "%f)
    return msg_sender, msg_subject, msg_content

@calc_time
def retrieve_sender_email(args, msg_sender):
    sender_email_raw = msg_sender
    if sender_email_raw is None:
        sender_email_raw = ' '
    pattern = re.compile('<(.*)>')
    sender_email = pattern.findall(sender_email_raw)
    if len(sender_email)>0:
        sender_email = sender_email[0].lower()
    else:
        if args.DEBUG:print("Error getting sender: %s"%sender_email_raw)
        sender_email = ""
    if args.DEBUG:print("Got sender:", sender_email)
    return sender_email

@calc_time
def judge_if_is_not_REply_or_others(args, sender_email, msg_subject, msg_subject_and_content):
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
    if len(re.findall('(^r|^re|^reply)[:| |-|,]', msg_subject, re.I))>0:    #If this is REply!! may cotian many irrelevant ships, so No!
        if 'ausca' in str(sender_email).lower() or '@swnav.com.tw' in str(sender_email).lower() or '@akij.net' in str(sender_email).lower():   #Ausca always with RE, so let it be.
            pass
        else:
            return False
    elif len(other_trashes_pattern.findall(msg_subject_and_content))>0:   #If others
        if args.DEBUG:print("Other trashes found:%s"%other_trashes_pattern.findall(msg_subject_and_content))
        return False
    else:
        return True

@calc_time
def judge_if_direct_bonding(args, sender_email, bonding_repository):
    #i, the keyword of bonding name.
    tmp = np.array([len(re.findall(i, sender_email, re.I)) for i in bonding_repository])
    if tmp.sum()>=1:
        direct_bonding = True
        if args.DEBUG:print("It's bonding", np.array(bonding_repository)[np.where(tmp==1)])
    else:
        direct_bonding = False
        if args.DEBUG:print("Not bonding")
    return direct_bonding

@calc_time
def retrieve_vessel(args, msg_content, vessels_patterns):
    vessels_name = []
    vessels_pattern_safe = vessels_patterns['safe']
    vessels_pattern_unsafe = vessels_patterns['unsafe']
    vessels_name_raw_safe = vessels_pattern_safe.findall(msg_content)   #return []
    vessels_name_raw_unsafe = vessels_pattern_unsafe.findall(msg_content)  #return [()]
    vessel_positions_safe = [i.start() for i in vessels_pattern_safe.finditer(msg_content)]
    vessel_positions_unsafe = [i.start() for i in vessels_pattern_unsafe.finditer(msg_content)]
    #If safe name occured then ignore unsafe name:
    redudant_name = []
    redudant_idx = []
    for idx,temp in enumerate(vessels_name_raw_unsafe):
        short_name = temp[1]
        pos = vessel_positions_unsafe[idx]
        if short_name in str(vessels_name_raw_safe):
            redudant_name.append(short_name)
            redudant_idx.append(pos)
    vessel_positions_unsafe = list(set(vessel_positions_unsafe)-set(redudant_idx)) 
    #Taken out names:
    vessel_positions = vessel_positions_safe + vessel_positions_unsafe
    vessels_name_raw = vessels_name_raw_safe + vessels_name_raw_unsafe
    if len(vessels_name_raw) == 0:
        pass
    else:
        for i in vessels_name_raw:
            for j in i:
                if j != '' and j not in redudant_name:
                    vessels_name.append(j.strip(' '))
    for idx,this_vessel_name in enumerate(vessels_name):
        vessels_name[idx] = this_vessel_name.upper()
    #order = vessels_name.index
    #vessels_name = list(set(vessels_name))
    #vessels_name.sort(key=order)
    if args.DEBUG:print("Got vessels name:", vessels_name)
    assert len(vessels_name) == len(vessel_positions), 'len vessel and len position must equal'
    return vessels_name, vessel_positions

@calc_time
def retrieve_skype(args, msg_content):
    #msg_content = SKYPE_TESTER
    skypes_id = []
    #tmp = re.compile('skype', re.I)
    #tt = tmp.search(msg_content)
    #if tt:
    #    print(msg_content[tt.span()[0]-min(25,tt.span()[0]-1):tt.span()[1]+25])
    patterns = []
    #skype id : xxx
    patterns.append('skype \(live\)[ ]?[:]?[ ]*([0-9a-z_\-:\.@]+)')
    patterns.append('msn/skype[ ]*[:]?[ ]*([0-9a-z_\-:\.@]+)')
    patterns.append('skype/msn[ ]*[:]?[ ]*([0-9a-z_\-:\.@]+)')
    patterns.append('skypeid[ ]?[:]?[ ]*([0-9a-z_\-:\.@]+)')
    patterns.append('skype id[ ]?[:]?[ ]*([0-9a-z_\-:\.@]+)')
    patterns.append('\(skypeid\)[ ]*[:]?[ ]*([0-9a-z_\-:\.@]+)')
    patterns.append('\(skype id\)[ ]*[:]?[ ]*([0-9a-z_\-:\.@]+)')
    patterns.append('skype id\)[ ]*[:]?[ ]*([0-9a-z_\-:\.@]+)')
    patterns.append('skypeid\)[ ]*[:]?[ ]*([0-9a-z_\-:\.@]+)')
    patterns.append('skype[ ]*[:]?[]*\+[ ]?([0-9a-z_\-:\.@]+)')
    patterns.append('\(skype\)[ ]*[:]?[ ]*([0-9a-z_\-:\.@]+)')
    patterns.append('([0-9a-z_\-:\.@]+)\(skypeid\)')
    patterns.append('([0-9a-z_\-:\.@]+)\(skype id\)')
    patterns.append('([0-9a-z_\-:\.@]+)\(skype\)')
    patterns.append('skype\)[ ]*[:]?[ ]*([0-9a-z_\-:\.@]+)')
    #Skype : xxxx
    patterns.append('skype[ ]*[:]?[ ]*([0-9a-z_\-:\.@]+)')
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
    if args.DEBUG:print("Got skype:", skypes_id)
    return skypes_id

@calc_time
def retrieve_pic_mailboxes(args, msg_content, sender_email):
    pattern = re.compile('([a-z0-9_\.]+@[a-z0-9\.]+\.[a-z]+)', re.I)
    pic_mailboxes  = pattern.findall(msg_content)
    pic_mailboxes += [sender_email]
    if args.DEBUG:print("Got mailboxes:", pic_mailboxes)
    for idx,this_pic_mailbox in enumerate(pic_mailboxes):
        pic_mailboxes[idx] = this_pic_mailbox.lower()
    return pic_mailboxes

@calc_time
def retrieve_pic_skype(args, msg_content, vessels_name, skypes_id):
    pic_skype = []
    #print(vessels_name, skypes_id)
    if len(vessels_name)>=1 and len(skypes_id)==1:
        if args.DEBUG:print('Got A pic skype for %s %s'%(vessels_name, skypes_id))
        pic_skype = skypes_id
    return pic_skype

@calc_time
def retrieve_mv_pic_pair(args, msg_content, vessels_name, vessel_positions, person_names, person_positions):
    mv_pic_pair = {}
    for i,mv in zip(vessel_positions, vessels_name): 
        above_person = ''
        below_person = ''
        above = np.where(i>np.array(person_positions))[0]
        below = np.where(i<np.array(person_positions))[0]
        if len(above)!=0:above_person=person_names[above[-1]]
        if len(below)!=0:below_person=person_names[below[0]]
        if not mv.upper() in str(list(mv_pic_pair.keys())).upper():
            mv_pic_pair[mv] = [above_person, below_person]
            while '' in mv_pic_pair[mv]:
                mv_pic_pair[mv].remove('')
    return mv_pic_pair

def parse_blob(args, msg_file, vessels_name, sender_email, skypes_id, pic_mailboxes, pic_skype):
    blob = {}
    blob['MSG_FILE'] = msg_file
    blob['MV'] = vessels_name
    blob['SENDER'] = sender_email
    blob['SKYPES'] = skypes_id
    blob['MAILBOXES'] = pic_mailboxes
    blob['PIC_SKYPE'] = pic_skype
    if args.DEBUG:print(blob)
    return blob

def retrieve_person_this_company(args, msg_sender, msg_content, company_person_skype_pattern):
    person_names = []
    person_positions = []
    which_company = np.array([this_company.lower() in msg_sender.lower() for this_company in company_person_skype_pattern.keys()])
    if which_company.sum()>1:
        print("Warning! Multi company person found!")
    elif which_company.sum()==1:
        which_company = np.array(list(company_person_skype_pattern.keys()))[which_company][0]
        person_names = company_person_skype_pattern[which_company].findall(msg_content)
        person_positions = [i.start() for i in company_person_skype_pattern[which_company].finditer(msg_content)]
    else:
        pass
    return person_names, person_positions

def get_bonding_repository(args):
    bonding_repository = list(set(pd.read_excel(DATA_PATH_PREFIX+"/person_names_repository.xls", index_col = 'bonding').index))
    #bonding_repository = open(DATA_PATH_PREFIX+"/bonding_repository.txt", 'r').readlines()
    #bonding_repository = ''.join(bonding_repository).split('\n')
    try:
        bonding_repository.remove('')
    except:
        pass
    return bonding_repository

def solve_one_msg(struct):
    blob = {}
    global TRASH_SENDER
    args, this_msg_file, FAILURE_LIST = struct[0], struct[1], struct[2]
    msg_sender, msg_subject, msg_content = parse_msg(args, this_msg_file)
    if msg_subject == False:
        FAILURE_LIST.append(this_msg_file)
        return blob
    #Now start:
    sender_email = retrieve_sender_email(args, msg_sender)
    if 'balticbroker@' in str(sender_email).lower() or 'noreply@' in str(sender_email).lower() or 'support@tnt' in str(sender_email).lower():
        return blob    #Just ignored these senders.
    if judge_if_is_not_REply_or_others(args, sender_email, msg_subject, msg_content)==True:
        vessels_name, vessel_positions = retrieve_vessel(args, msg_content, vessels_patterns)
        if len(vessels_name)==0:return blob    #If no vessel found, just return
        person_names, person_positions = retrieve_person_this_company(args, msg_sender, msg_content, company_person_skype_pattern)
        skypes_id = retrieve_skype(args, msg_content)
        pic_skype = retrieve_pic_skype(args, msg_content, vessels_name, skypes_id)
        mv_pic_pair = retrieve_mv_pic_pair(args, msg_content, vessels_name, vessel_positions, person_names, person_positions)
        pic_skype = mv_pic_pair
        if judge_if_direct_bonding(args, sender_email, bonding_repository) is True:
            #embed()   #company_person_skype
            print(sender_email)
            pic_mailboxes = retrieve_pic_mailboxes(args, msg_content, sender_email)
        else:   #When not direct bonding
            sender_email = '_BROKER_SENDER_'
            skypes_id = ['_BROKER_SKYPES_']
            pic_mailboxes = ['_BROKER_MAILBOXES_']
            pic_skype.update({'_BROKER_TOKEN_':None})   #len=1 for direct, or len=2 for shit broker's msg
    #    direct_bonding = True
        blob = parse_blob(args, this_msg_file, vessels_name, sender_email, skypes_id, pic_mailboxes, pic_skype)
    else:
        if args.DEBUG:print("This is Reply or with trashes! pass")
    return blob

def concat_blobs_through_history(args, blobs):
    for _idx_, blob in enumerate(blobs):
        if blob == {}:continue
        #Now that BLOB is here anyway, decide what to do:
        #SUBSTUTIVE: (for sender and pic_skype)
        for mv in blob['MV']:
            if mv not in MV_SENDER_BLOB.keys():       #First time show up
                MV_SENDER_BLOB[mv] = blob['SENDER'] 
            else:
                if blob['SENDER'] == '_BROKER_SENDER_':  #Non-direct will give ['_BROKER_SENDER_'].
                    pass
                else:
                    MV_SENDER_BLOB[mv] = blob['SENDER'] 
        for mv in blob['MV']:
            if mv not in MV_SKYPE_BLOB.keys():   #First time show up
                MV_SKYPE_BLOB[mv] = blob['PIC_SKYPE'][mv]
            else:
                if blob['SENDER'] == '_BROKER_SENDER_':   #JUDGE IF GIVEN BY SHIT BROKER USING ITS SENDER TOKEN
                    if '_BROKER_TOKEN_' in MV_SKYPE_BLOB[mv] and len(blob['PIC_SKYPE'])>1 and blob['PIC_SKYPE'][mv] != []:
                        MV_SKYPE_BLOB[mv] = blob['PIC_SKYPE'][mv]  #Shit for shit.
                    else:
                        pass
                else:   #when _BROKER_SENDER not in it
                    if blob['PIC_SKYPE'][mv] != []:
                        MV_SKYPE_BLOB[mv] = blob['PIC_SKYPE'][mv] #Update pic only when actually found and is not given by shit broker 
        #ACCUMULATIVE:
        sender = blob['SENDER']
        #Worryring about shit broker? Will use as key, so let it be~
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
    return MV_SENDER_BLOB, SENDER_MAILBOXES_BLOB, SENDER_SKYPES_BLOB, MV_SKYPE_BLOB

if __name__ == "__main__":
    print("Start principle net...")

    #Config:
    parser = argparse.ArgumentParser()
    parser.add_argument("-D", '--DEBUG', action='store_true', default=False)
    parser.add_argument("-S", '--FROM_SCRATCH', action='store_true', default=False)
    parser.add_argument("-Q", '--QUICK_COMPANY_DATA', action='store_true', default=False)
    args = parser.parse_args()
    FROM_SCRATCH = args.FROM_SCRATCH
    DATA_PATH_PREFIX = './data/data_bonding_net/'
    BLACKLIST_MAILBOXES = [i.strip('\n').lower() for i in open(DATA_PATH_PREFIX+"/pic_blacklist.txt").readlines()]
    WRONG_SKYPE_PAIR = pd.read_csv(DATA_PATH_PREFIX+"/problem_skype.csv", index_col=0) 
    #From scratch? Checkpoint?
    MV_SENDER_BLOB = {}
    SENDER_MAILBOXES_BLOB = {}
    SENDER_SKYPES_BLOB = {}
    MV_SKYPE_BLOB = {}
    if not MP:
        TRASH_SENDER = []
        FAILURE_LIST = []
    else:   #MP    
        manager = Manager()
        TRASH_SENDER = manager.list()
        FAILURE_LIST = manager.list()

    if FROM_SCRATCH:
        print("Run from scratch, moving msgs to work place...")
        consumed_files = glob.glob("%s/msgs/consumed/*.msg"%DATA_PATH_PREFIX)
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
    msg_files = glob.glob(DATA_PATH_PREFIX+"/msgs/*.msg")
    msg_files.sort()
    #Repos:
    bonding_repository = get_bonding_repository(args)
    vessels_repository, vessels_patterns = get_vessels_repository_and_patterns(args)
    company_person_skype, company_person_skype_pattern = get_person_name_repository_and_patterns(args)

    #Loop over msgs:
    blobs = []
    if MP:
        pool = Pool(processes=int(cpu_count()/2))   #cpu_count()/2, 既省电也高效。
        struct_list = []
        for i in range(len(msg_files)):
            struct_list.append([args, msg_files[i], FAILURE_LIST])
        rs = pool.map_async(solve_one_msg, struct_list)
        while (True):
          if (rs.ready()): break
          remaining = rs._number_left
          print("Waiting for", remaining, "tasks to complete...")
          time.sleep(10)
        blobs = rs.get()   #HISTORY ORDER IS WITHIN!! VERY CONVINIENT
    else:
        for this_msg_file in tqdm.tqdm(msg_files):
            struct_this = [args, this_msg_file, FAILURE_LIST]
            blob = solve_one_msg(struct_this)
            blobs += [blob]
    try:    #Try to give a log
        print("Writing blobs to log....")
        with open('output/log.log', 'w') as f:
            f.write(str(blobs))
    except Exception as e:
        print("Error writing log, pass", e)
    #Loop over history:
    MV_SENDER_BLOB, SENDER_MAILBOXES_BLOB, SENDER_SKYPES_BLOB, MV_SKYPE_BLOB = concat_blobs_through_history(args, blobs)

    #POSTPROCESSING:
    #embed()
    #for sender_skypes, switch right or wrong
    for sender in SENDER_SKYPES_BLOB.keys():
        for idx,this_skype_id in enumerate(SENDER_SKYPES_BLOB[sender]):
            if this_skype_id in list(WRONG_SKYPE_PAIR.index):
                SENDER_SKYPES_BLOB[sender][idx] = WRONG_SKYPE_PAIR.loc[this_skype_id].right
    #for pic_skype, switch right or wrong
    for mv in MV_SKYPE_BLOB.keys():
        for idx,this_skype_id in enumerate(MV_SKYPE_BLOB[mv]):
            if this_skype_id in list(WRONG_SKYPE_PAIR.index):
                MV_SKYPE_BLOB[mv][idx] = WRONG_SKYPE_PAIR.loc[this_skype_id].right
    #for pic_skype, -TOKEN
    for mv in MV_SKYPE_BLOB.keys():
        MV_SKYPE_BLOB[mv] = list(set(MV_SKYPE_BLOB[mv])-set(["_BROKER_TOKEN_"]))
    #for Mailboxes, Blacklist and -skype and -bancosta:
    for sender in SENDER_MAILBOXES_BLOB.keys():
        del_these = []
        for this_mailbox in SENDER_MAILBOXES_BLOB[sender]:
            if 'bancosta' in this_mailbox.lower():
                del_these.append(this_mailbox)
        SENDER_MAILBOXES_BLOB[sender] = list(set(SENDER_MAILBOXES_BLOB[sender])-set(SENDER_SKYPES_BLOB[sender])-set(BLACKLIST_MAILBOXES)-set(del_these))
    print("Failures %s:"%len(FAILURE_LIST), set(FAILURE_LIST))

    #embed()
    #Generate outputs:
    output_MV_SENDER = pd.DataFrame({'MV':list(MV_SENDER_BLOB.keys()), 'SENDER':list(MV_SENDER_BLOB.values())})
    output_SENDER_MAILBOXES = pd.DataFrame({'SENDER':list(SENDER_MAILBOXES_BLOB.keys()), 'MAILBOXES':list(SENDER_MAILBOXES_BLOB.values())})[['SENDER','MAILBOXES']]
    output_SENDER_SKYPES = pd.DataFrame({'SENDER':list(SENDER_SKYPES_BLOB.keys()), 'SKYPES':list(SENDER_SKYPES_BLOB.values())})
    output_MV_SKYPE = pd.DataFrame({'MV':list(MV_SKYPE_BLOB.keys()), 'PIC_SKYPE':list(MV_SKYPE_BLOB.values())})
    output_MV_SENDER_MAILBOXES_SKYPE = pd.DataFrame({'MV':list(MV_SENDER_BLOB.keys()), 'SENDER':list(MV_SENDER_BLOB.values()), 'MAILBOXES':list(map(SENDER_MAILBOXES_BLOB.get, MV_SENDER_BLOB.values())), 'SKYPES':list(map(SENDER_SKYPES_BLOB.get, MV_SENDER_BLOB.values())), 'PIC_SKYPE':list(MV_SKYPE_BLOB.values())})[['MV','SENDER','MAILBOXES','SKYPES','PIC_SKYPE']]
    TRASH_SENDER = pd.DataFrame({'TRASH_SENDER': list(TRASH_SENDER)})
    ok = pd.DataFrame({ 'ok': list(SENDER_MAILBOXES_BLOB.keys())})
    try:
        output_MV_SENDER.to_csv('output/core_MV_SENDER.csv', index=False)  
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
