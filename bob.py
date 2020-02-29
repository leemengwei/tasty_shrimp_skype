#Skype version 8.56.0.100 2019
from skpy import Skype
from IPython import embed
import os,sys
import pandas as pd
import tqdm
import time
import datetime

def get_template_to_send(template_file):
    print("Getting template...")
    content = open(template_file, 'r').readlines()
    content = ''.join(content).replace('\n', '\t\n')
    return content

def relentless_login_web_skype(username, password, sleep=0):
    while 1:
        n = 0
        while 1:
            n += 1
            print("Logging in as %s"%username)
            time.sleep(sleep)
            try:
                sk = Skype(username, password) # connect to Skype
                return sk
            except Exception as e:
                time.sleep(0.5)
                print(e)
                print("Force login again...", n)

def relentlessly_get_blob_by_id(sk, this_id, username, password):
    while 1:
        n = 0
        while (n<30):
            n += 1
            print("Relent getting blob, retry:%s"%n)
            try:
                blob = sk.contacts[this_id]
                return sk, blob
            except Exception as e:
                time.sleep(0.5)
                last_e = e
                sk.conn.verifyToken(sk.conn.tokens)
        print("Doing re-log in Due to: %s"%last_e)
        sk = relentless_login_web_skype(username, password, sleep=3)

def relentlessly_get_commander_message(sk, commander_id, username, password):
    while 1:
        n = 0
        while (n<3):    #if getMsg fails, it usually stuck a longtime, so be quick
            n += 1
            print("Relent getting marshal, retry:%s"%n)
            try:
                chat_messages = sk.contacts[commander_id].chat.getMsgs()
                return chat_messages
            except Exception as e:
                time.sleep(0.5)
                last_e = e
                sk.conn.verifyToken(sk.conn.tokens)
        print("Doing re-log in Due to: %s"%last_e)
        sk = relentless_login_web_skype(username, password, sleep=3)

def relentlessly_chat_by_blob(sk, blob, message, username, password, this_id):
    give_up = 0
    while give_up<3: 
        give_up += 1
        n = 0
        while (n<30):
            print("Relent chating, retry:%s"%n)
            n += 1
            try:
                blob_chat = blob.chat
                blob_chat.sendMsg(message)
                return
            except Exception as e:
                time.sleep(0.5)
                last_e = e
                sk.conn.verifyToken(sk.conn.tokens)
        #If still can't chat for 60 times
        print("Doing re-log in Due to: %s"%last_e)
        sk = relentless_login_web_skype(username, password, sleep=3)
        sk, blob = relentlessly_get_blob_by_id(sk, this_id, username, password)
   
def check_invalid_account(sk, all_target_people):
    print("Checking invalid account...")
    for idx,i in tqdm.tqdm(enumerate(all_target_people)):
        print(idx)
        sk, blob = relentlessly_get_blob_by_id(sk, i)
        if blob is None:
            print("Removing invalid %s"%i)
            all_target_people.remove(i)
        else:
            pass
    return all_target_people

def get_all_target_people(sk, username, additional_contacts_path, remove_contacts_path):    
    if DRY_RUN:
        dry_target_people = open("./data/%s/mengxuan.txt"%username, 'r').readlines()
        dry_target_people = ''.join(dry_target_people).split('\n')[:]
        print("Returing dry:%s"%dry_target_people)
        return dry_target_people
    #Get sync contacts
    print("Getting contacts...")
    sk.contacts.sync()
    all_contacts = sk.contacts.contactIds
    with open("data/%s/raw_skype_contacts_%s.txt"%(username, str(datetime.datetime.today()).split(' ')[0]), 'w') as f:
        for i in all_contacts:
            f.write(i+ '\n')
    additional_contacts = open(additional_contacts_path, 'r').readlines()
    additional_contacts = ''.join(additional_contacts).split('\n')[:]
    removed_contacts = open(remove_contacts_path, 'r').readlines()
    removed_contacts = ''.join(removed_contacts).split('\n')[:]
    all_target_people = additional_contacts
    #all_target_people = all_contacts + additional_contacts
    all_target_people = list(set(all_target_people))
    for i in removed_contacts:
        try:
            print("Removing:%s"%i)
            all_target_people.remove(i)
        except:
            pass
    if CHECK_CONTACTS_VALID:
        all_target_people = check_invalid_account(sk, all_target_people)
    print("%s people returned,%s people removed"%(len(all_target_people), len(removed_contacts)))
    #print(all_target_people)
    return all_target_people

def parse_infos(sk, all_target_people, template_contents, username, password):
    print("Parsing infos...")
    if PARSE_FROM_ZERO or DRY_RUN:   #which takes long time
        data = {'id':all_target_people, 'name':None, 'location':None, 'language':None, 'avatar':None, 'mood':None, 'phones':None, 'birthday':None, 'authorised':None, 'blocked':None, 'favourite':None, "contents":None, "chat_times_sent":0, 'times':None, 'misc':None}
        pd_blobs = pd.DataFrame(data)
        for each_id in tqdm.tqdm(all_target_people):
            sk, blob = relentlessly_get_blob_by_id(sk, each_id, username, password)
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
            sk, blob = relentlessly_get_blob_by_id(sk, each_id, username, password)
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
 
def send_messages(sk, pd_blobs, external_content=None):
    print("Sending messages...")
    for row in tqdm.tqdm(range(len(pd_blobs))):
        this_id = pd_blobs.iloc[row].id
        this_name = pd_blobs.name[row]
        this_info = pd_blobs.iloc[row].contents if external_content is None else "Hi %s, this is zombie bob, undying :)\n%s"%(this_name, external_content)
        print("Now on: %s"%this_name)
        sk, blob = relentlessly_get_blob_by_id(sk, this_id, username, password)
        if blob is None:
            continue
        else:
            #Pressure test mode:
            n = 0
            while PRESSURE_TEST:
                message = "Pressure test...%s"%n
                relentlessly_chat_by_blob(sk, blob, message, username, password, this_id)
                pd_blobs.chat_times_sent.iat[row] += 1
                n += 1
                print("Message sent for %s:%s"%(this_id, message))
            #Normal mode:
            relentlessly_chat_by_blob(sk, blob, this_info, username, password, this_id)
            pd_blobs.chat_times_sent.iat[row] += 1
            print("Message sent for %s:%s"%(this_id, this_info))
    return

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
    #ch = sk.contacts[person_talk_to].chat # 1-to-1 conversation

    #ch.sendMsg("Hi")
    #ch.sendFile(open("song.mp3", "rb"), "song.mp3") # file upload
    #ch.sendContact(sk.contacts["daisy.5"]) # contact sharing

    #ch.getMsgs()
    return

if __name__ == "__main__":
    PRESSURE_TEST = False
    #PRESSURE_TEST = True
    CHECK_CONTACTS_VALID = False
    #CHECK_CONTACTS_VALID = True
    PARSE_FROM_ZERO = False
    PARSE_FROM_ZERO = True
    DRY_RUN = False
    DRY_RUN = True
    
    if PRESSURE_TEST:
        DRY_RUN = True
    print("Starting...")
    username = '18601156335'
    #username = 'mengxuan@bancosta.com'
    password = 'lmw196411'
    #password = 'Bcchina2020'
 
    additional_contacts_path = 'data/%s/saved_contacts.txt'%username
    remove_contacts_path = 'data/%s/removed_contacts.txt'%username
    template_file = "data/%s/content.txt"%username
    if not os.path.exists("data/%s/"%username):
        os.mkdir("data/%s/"%username)
    
    #Log in:
    sk = relentless_login_web_skype(username, password)

    #Get all targets:
    all_target_people = get_all_target_people(sk, username, additional_contacts_path, remove_contacts_path)
    #sys.exit()
 
    #Get templates:
    template_contents = get_template_to_send(template_file)

    #Parse together:
    pd_blobs = parse_infos(sk, all_target_people, template_contents, username, password)
    #embed()
    #sys.exit()

    #Wait signal:
    commander_id = 'live:mengxuan_9'
    while len(sk.contacts['live:mengxuan_9'].chat.getMsgs()):
        sk.contacts['live:mengxuan_9'].chat.getMsgs()
    print("Old messages ignored.")
    while 1:
        chat_messages = relentlessly_get_commander_message(sk, commander_id, username, password)
        print("%s new messages with marshal"%len(chat_messages))
        for message_this in chat_messages:
            if ("TOKEN" in message_this.content) and (message_this.userId==commander_id):
                daily_report = message_this.content.strip("TOKEN").replace("TOKEN",'')
                print("Token caught")
                #Send for every one:
                send_messages(sk, pd_blobs, external_content = daily_report)
 
        sys.stdout.flush()
        time.sleep(4)
   
    sys.exit()
   
    
    
   
