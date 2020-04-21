#Skype version 8.56.0.100 2019
from skpy import Skype
from IPython import embed
import os,sys
import pandas as pd
import tqdm
import time
import datetime
import timeout_decorator
from multiprocessing.pool import Pool
import numpy as np

def flusher(fun):
    print("In function: ", fun.__name__)
    sys.stdout.flush()
    return fun

@flusher
def get_template_to_send(template_file):
    print("Getting template...")
    content = open(template_file, 'r').readlines()
    content = ''.join(content).replace('\n', '\t\n')
    sys.stdout.flush()
    return content

@flusher
def relentless_login_web_skype(username, password, sleep=0, WAIT_TIME=45):
    @timeout_decorator.timeout(WAIT_TIME)
    def auto_timeout_login(username, password):
        sk = Skype(username, password) # connect to Skype
        return sk
    while 1:
        n = 0
        while 1:
            n += 1
            print("Logging in as %s"%username)
            sys.stdout.flush()
            time.sleep(sleep)
            try:
                sk = auto_timeout_login(username, password) # connect to Skype
                return sk
            except Exception as e:
                time.sleep(1)
                print(e)
                print("Force login again...", n)

@flusher
def relentlessly_get_blob_by_id(sk, this_id, username, password):
    @timeout_decorator.timeout(WAIT_TIME)
    def auto_timeout_getblob(sk, this_id):
        blob = sk.contacts[this_id]
        return blob
    while 1:
        n = 0
        while (n<3):
            n += 1
            print("Relent getting blob %s, retry:%s"%(this_id, n))
            sys.stdout.flush()
            try:
                blob = auto_timeout_getblob(sk, this_id)
                return blob, sk
            except Exception as e:
                time.sleep(0.5)
                last_e = e
                sk.conn.verifyToken(sk.conn.tokens)
        print("Doing re-log (get blob) in Due to: %s"%last_e)
        sk = relentless_login_web_skype(username, password, sleep=WAIT_TIME)

@flusher
def relentlessly_get_commander_message(sk, commander_id, username, password):
    @timeout_decorator.timeout(WAIT_TIME)
    def auto_timeout_getMsgs(commander_id):
        chat_messages = sk.contacts[commander_id].chat.getMsgs()
        return chat_messages
    while 1:
        n = 0
        while (n<5): 
            n += 1
            print("Relent getting command %s, retry:%s"%(commander_id, n))
            sys.stdout.flush()
            chat_messages = []
            try:
                tmp = auto_timeout_getMsgs(commander_id)
                chat_messages += tmp
                while len(tmp)>0:    #for each commander, get ALL its messages.
                    tmp = auto_timeout_getMsgs(commander_id)
                    chat_messages += tmp
                return chat_messages, sk
            except Exception as e:
                time.sleep(0.5)
                last_e = e
                sk.conn.verifyToken(sk.conn.tokens)
                #if "Response" in str(last_e) and '40' in str(last_e):
                #    print("Cannot get MSG by id:", commander_id, 'due to %s'%last_e)
                #    return []
        print("Doing re-log (get command) in Due to: %s"%last_e)
        sk = relentless_login_web_skype(username, password, sleep=WAIT_TIME)

@flusher
def max_giveup_chat_by_blob(sk, blob, message, username, password, this_id):
   @timeout_decorator.timeout(WAIT_TIME)
   def auto_timeout_chat(message):
       tmp_len = 1
       history_chats = []
       while tmp_len>0: #check historical messages
           tmp = blob.chat.getMsgs()
           history_chats += tmp
           tmp_len = len(tmp)
       if message in str(history_chats):
           print("Already sent,", this_id)
           return
       else:
           blob.chat.sendMsg(message)
           return
   give_up = 0
   while give_up<1: 
        give_up += 1
        n = 0
        while (n<2):
            print("Relent chating, retry:%s"%n)
            sys.stdout.flush()
            n += 1
            try:
                auto_timeout_chat(message)
                return sk
            except Exception as e:
                time.sleep(0.5)
                last_e = e
                if "403" in str(last_e):
                    print("Redeem 403 as successful", this_id)
                    return sk
                sk.conn.verifyToken(sk.conn.tokens)
        #If still can't chat for times
        print("Doing re-log in Due to: %s"%last_e)
        sk = relentless_login_web_skype(username, password, sleep=3)
        blob, sk = relentlessly_get_blob_by_id(sk, this_id, username, password)
   print("Given up on %s"%this_id)
   return sk

@flusher
def ideal_pool_chat_by_blob(struct):
    @timeout_decorator.timeout(WAIT_TIME)
    def auto_timeout_blob_and_chat(skype_id, message):
        blob = sk.contacts[skype_id]
        sys.stdout.flush()
        tmp_len = 1
        history_chats = []
        while tmp_len>0: #check historical messages
            tmp = blob.chat.getMsgs()
            history_chats += tmp
            tmp_len = len(tmp)
        if message in str(history_chats):
            print("*SKYPE* Pool Already sent,", skype_id)
            return
        else:
            print("*SKYPE* Pool Sending to %s"%skype_id)
            blob.chat.sendMsg(message)
        #if DRY_RUN and skype_id=='live:a4333d00d55551e': #me_id
        #    print(Failure_on_intension)
        return
    skype_id, message, sk = struct[0], struct[1], struct[2]
    try:
        auto_timeout_blob_and_chat(skype_id, message)
        print("Okay", skype_id)
        return True
    except Exception as e:
        print("When sending %s,"%skype_id, e)
        if '403' in str(e):
            #print("Redeem 403 as successful", skype_id)
            #return True
            pass
        sys.stdout.flush()
        return False

@flusher
def check_invalid_account(sk, all_target_people):
    print("Checking invalid account...")
    for idx,i in tqdm.tqdm(enumerate(all_target_people)):
        print(idx)
        blob, sk = relentlessly_get_blob_by_id(sk, i)
        if blob is None:
            print("Removing invalid %s"%i)
            sys.stdout.flush()
            all_target_people.remove(i)
        else:
            pass
    return all_target_people

@flusher
def get_all_target_people(sk, username, additional_contacts_path, remove_contacts_path):    
    if DRY_RUN:
        dry_target_people = open("./data/%s/DRY_RUN_mengxuan"%username, 'r').readlines()
        dry_target_people = ''.join(dry_target_people).split('\n')[:-1]
        print("Returing dry:%s"%dry_target_people)
        sys.stdout.flush()
        return dry_target_people
    #Get sync contacts
    print("Getting contacts...")
    sk.contacts.sync()
    skype_contacts = sk.contacts.contactIds
    with open("data/%s/raw_skype_contacts_%s.txt"%(username, str(datetime.datetime.today()).split(' ')[0]), 'w') as f:
        for i in skype_contacts:
            f.write(i+ '\n')
    additional_contacts = open(additional_contacts_path, 'r').readlines()
    additional_contacts = ''.join(additional_contacts).split('\n')[:]
    removed_contacts = open(remove_contacts_path, 'r').readlines()
    print(removed_contacts)
    removed_contacts = ''.join(removed_contacts).split('\n')[:]
    all_target_people = list(set(skype_contacts)&set(additional_contacts) - set(removed_contacts))
    if CHECK_CONTACTS_VALID:
        all_target_people = check_invalid_account(sk, all_target_people)
    print("%s people returned,%s people removed"%(len(all_target_people), len(removed_contacts)))
    #print(all_target_people)
    return all_target_people

@flusher
def parse_infos(sk, all_target_people, template_contents, username, password):
    print("Parsing infos...")
    if PARSE_FROM_ZERO or DRY_RUN:   #which takes long time
        data = {'id':all_target_people, 'name':None, 'location':None, 'language':None, 'avatar':None, 'mood':None, 'phones':None, 'birthday':None, 'authorised':None, 'blocked':None, 'favourite':None, "contents":None, "chat_times_sent":0, 'times':None, 'misc':None}
        pd_blobs = pd.DataFrame(data)
        for each_id in tqdm.tqdm(all_target_people):
            blob, sk = relentlessly_get_blob_by_id(sk, each_id, username, password)
            if blob is None:
                continue
            which_row = pd_blobs[pd_blobs.id==each_id].index
            for its_attr in blob.attrs:
                if its_attr == 'name':
                    pd_blobs.loc[which_row, its_attr] = str(eval("blob.%s"%its_attr)).split(' ')[0]             #when parse name, we take first name.
                else:
                    pd_blobs.loc[which_row, its_attr] = str(eval("blob.%s"%its_attr))
            pd_blobs.loc[which_row, 'contents'] = "Hi %s,\n"%pd_blobs.name[which_row[0]]+template_contents
        if not DRY_RUN:
            pd_blobs.to_csv('data/%s/contacts_profile.csv'%username, index=None)
    else:
        pd_blobs_read = pd.read_csv("data/%s/contacts_profile.csv"%username)
        missed_targets = list(set(list(all_target_people))-set(list(pd_blobs_read.id)))
        surplus_targets = list(set(list(pd_blobs_read.id))-set(list(all_target_people)))
        data = {'id':missed_targets, 'name':None, 'location':None, 'language':None, 'avatar':None, 'mood':None, 'phones':None, 'birthday':None, 'authorised':None, 'blocked':None, 'favourite':None, "contents":None, "chat_times_sent":0, 'times':None, 'misc':None}
        pd_blobs_missed = pd.DataFrame(data)
        print("Updating misses:", missed_targets)
        for each_id in tqdm.tqdm(missed_targets):
            blob, sk = relentlessly_get_blob_by_id(sk, each_id, username, password)
            if blob is None:
                continue
            which_row = pd_blobs_missed[pd_blobs_missed.id==each_id].index
            for its_attr in blob.attrs:
                if its_attr == 'name':
                    pd_blobs_missed.loc[which_row, its_attr] = str(eval("blob.%s"%its_attr)).split(' ')[0]             #when parse name, we take first name.
                else:
                    pd_blobs_missed.loc[which_row, its_attr] = str(eval("blob.%s"%its_attr))
            pd_blobs_missed.loc[which_row, 'contents'] = "Hi %s\n"%pd_blobs_missed.name[which_row[0]]+template_contents
        pd_blobs = pd_blobs_read.append(pd_blobs_missed, ignore_index=True)
        pd_blobs.to_csv('data/%s/contacts_profile.csv'%username, index=None)
        print("contacts_profile updated...")
    return pd_blobs

def messages_wrapper_pool(sk, username, password, all_target_people, external_content):
    #sk = relentless_login_web_skype(username, password, sleep=0):
    pool = Pool(processes=8)
    struct_list = []
    for i,j,k in zip(all_target_people, [external_content]*len(all_target_people), [sk]*len(all_target_people)):
        struct_list.append([i,j,k])
    if PRESSURE_TEST:struct_list *= 25
    n = 0
    while n<3 and len(struct_list)>0:
        status = pool.map(ideal_pool_chat_by_blob, struct_list)
        struct_list = np.array(struct_list)[np.where(np.array(status)==False)].tolist()
        if len(struct_list)>0:
            failed_name = np.array(struct_list)[:,0].tolist()
            print("Remaining (retrying on):", failed_name, n)
            time.sleep(WAIT_TIME)
            sk = relentless_login_web_skype(username, password)
            sk.conn.verifyToken(sk.conn.tokens)
            for _struct_ in struct_list:
                _struct_[-1] = sk      #Renew sk for pools after login
            if len(struct_list)<30:
                n += 1
            else:
               print("HeHe too much failures, n+=0.5")
               n += 0.5
    if len(struct_list)>0:
        print("Failure (given up) struct:", failed_name)
    pool.close()
    pool.join()
    return sk

@flusher
def messages_wrapper_simple(sk, all_target_people, external_content):
    print("Sending messages simple...")
    for this_id in tqdm.tqdm(all_target_people):
        this_info = external_content
        print("Now on: %s"%this_id)
        sys.stdout.flush()
        blob, sk = relentlessly_get_blob_by_id(sk, this_id, username, password)
        if blob is None:
            continue
        else:
            sk = max_giveup_chat_by_blob(sk, blob, this_info, username, password, this_id)
            print("Message sent for %s:%s"%(this_id, this_info))
    return sk


#@flusher
#def messages_wrapper_old(sk, pd_blobs, external_content=None):
#    print("Sending messages...")
#    for row in tqdm.tqdm(range(len(pd_blobs))):
#        this_id = pd_blobs.iloc[row].id
#        this_name = pd_blobs.name[row]
#        this_info = pd_blobs.iloc[row].contents if external_content is None else external_content
#        print("Now on: %s"%this_name)
#        sys.stdout.flush()
#        blob, sk = relentlessly_get_blob_by_id(sk, this_id, username, password)
#        if blob is None:
#            continue
#        else:
#            if str(blob.name) == '':
#                continue
#            #Pressure test mode:
#            n = 0
#            while PRESSURE_TEST:
#                message = "Pressure test...%s"%n
#                sk = max_giveup_chat_by_blob(sk, blob, message, username, password, this_id)
#                pd_blobs.chat_times_sent.iat[row] += 1
#                n += 1
#                print("Message sent for %s:%s"%(this_id, message))
#            #Normal mode:
#            sk = max_giveup_chat_by_blob(sk, blob, this_info, username, password, this_id)
#            pd_blobs.chat_times_sent.iat[row] += 1
#            print("Message sent for %s:%s"%(this_id, this_info))
#    return sk

@flusher
def misc():
    #person_talk_to = {}
    #person_talk_to['nickname'] = 'test_for_py'
    #person_talk_to['phone'] = '8613120090157'
    #person_talk_to['id'] = 'live:892bfe64f9296876'

    #sk.user # you
    #sk.contacts # your contacts
    #sk.chats # your conversations

    #ch = sk.contacts.contact(person_talk_to['id'])  #by id!
    #ch = sk.contacts.search('8613120090157')
    #ch = sk.contacts.user(person_talk_to['id']) #so far, same as .contact
    
    #ch = sk.chats.create([person_talk_to]) # new group conversation

    #ch.sendFile(open("song.mp3", "rb"), "song.mp3") # file upload
    #ch.sendContact(sk.contacts["daisy.5"]) # contact sharing

    #ch.getMsgs()
    return

if __name__ == "__main__":
    WAIT_TIME = 55
    PRESSURE_TEST = False
    #PRESSURE_TEST = True
    CHECK_CONTACTS_VALID = False
    #CHECK_CONTACTS_VALID = True
    PARSE_FROM_ZERO = False
    PARSE_FROM_ZERO = True
    DRY_RUN = False
    #DRY_RUN = True
    
    if PRESSURE_TEST:
        DRY_RUN = True
    print("Starting...")
    #username = '18601156335'
    username = 'mengxuan@bancosta.com'
    #password = 'lmw196411'
    password = 'Bcchina2020'
    me_id = "live:a4333d00d55551e"

    additional_contacts_path = 'data/%s/saved_contacts'%username
    remove_contacts_path = 'data/%s/removed_contacts'%username
    template_file = "data/%s/content"%username
    if not os.path.exists("data/%s/"%username):
        os.mkdir("data/%s/"%username)

    #Report state:
    print("\n**************STATE*************************************")
    print("PRESSURE_TEST:",PRESSURE_TEST)
    print("CHECK_CONTACTS_VALID:",CHECK_CONTACTS_VALID)
    print("PARSE_FROM_ZERO:",PARSE_FROM_ZERO)
    print("DRY_RUN:",DRY_RUN)
    print("username:",username)
    print("password:",password)
    print("additional_contacts_path:",additional_contacts_path)
    print("remove_contacts_path:",remove_contacts_path)
    print("template_file:",template_file)
    print("**********************************************************\n")
    
    #Log in:
    sk = relentless_login_web_skype(username, password)
    
    #Get all targets:
    all_target_people = get_all_target_people(sk, username, additional_contacts_path, remove_contacts_path)
    #sys.exit()
 
    #Get templates:
    template_contents = get_template_to_send(template_file)

    #Parse together:
    pd_blobs = parse_infos(sk, all_target_people, template_contents, username, password)
    #sys.exit()

    #Wait signal:
    commander_ids = ['live:mengxuan_9', 'live:892bfe64f9296876', me_id]
    #ignore_old(sk, commander_ids, username, password)
    while 1:
        for commander_id in commander_ids:
            chat_messages, sk = relentlessly_get_commander_message(sk, commander_id, username, password)
            #print("New messages with commander %s:"%commander_id, chat_messages)
            #Whether signal:
            for message_this in chat_messages:
                seconds_that_passed = (datetime.datetime.now() - message_this.time).days*24*3600+(datetime.datetime.now() - message_this.time).seconds - 8*3600   #Greenwich time
                #print('seconds_that_passed', seconds_that_passed)
                if (" TOKEN " in message_this.content) and (message_this.userId in commander_ids) and (seconds_that_passed<WAIT_TIME):
                    print("Token caught")
                    daily_report = message_this.content.replace(" TOKEN ",'')
                    #Send for every one:
                    #sk = messages_wrapper_simple(sk, all_target_people, external_content = daily_report)
                    sk = messages_wrapper_pool(sk, username, password, all_target_people, external_content = daily_report)
                    print("Now resting ...")
                    time.sleep(WAIT_TIME)   #TOKEN is removed , now must safe, just sleep
        time.sleep(7)
   
    sys.exit()
   
    
    
   
