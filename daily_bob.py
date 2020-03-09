#Skype version 8.56.0.100 2019
from skpy import Skype
from IPython import embed
import os,sys
import pandas as pd
import tqdm
import time
import datetime
import timeout_decorator

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
def relentless_login_web_skype(username, password, sleep=0):
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
        print("Doing re-log in Due to: %s"%last_e)
        sk = relentless_login_web_skype(username, password, sleep=3)

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
            try:
                chat_messages = auto_timeout_getMsgs(commander_id)
                return chat_messages, sk
            except Exception as e:
                time.sleep(0.5)
                last_e = e
                sk.conn.verifyToken(sk.conn.tokens)
                #if "Response" in str(last_e) and '40' in str(last_e):
                #    print("Cannot get MSG by id:", commander_id, 'due to %s'%last_e)
                #    return []
        print("Doing re-log (rest first) in Due to: %s"%last_e)
        sk = relentless_login_web_skype(username, password, sleep=WAIT_TIME)

@flusher
def max_giveup_chat_by_blob(sk, blob, message, username, password, this_id):
   @timeout_decorator.timeout(WAIT_TIME)
   def auto_timeout_chat(message):
       blob.chat.sendMsg(message)
       return
   give_up = 0
   while give_up<2: 
        give_up += 1
        n = 0
        while (n<2):
            print("Relent chating, retry:%s"%n)
            sys.stdout.flush()
            n += 1
            try:
                auto_timeout_chat(message)
                return
            except Exception as e:
                time.sleep(0.5)
                last_e = e
                #if "Response" in str(last_e):
                #    print("Cannot receive by id:", this_id, 'due to %s'%last_e)
                #    return
                sk.conn.verifyToken(sk.conn.tokens)
        #If still can't chat for times
        print("Doing re-log in Due to: %s"%last_e)
        sk = relentless_login_web_skype(username, password, sleep=3)
        blob, sk = relentlessly_get_blob_by_id(sk, this_id, username, password)
   print("Given up on %s"%this_id)

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
        dry_target_people = open("./data/%s/mengxuan.txt"%username, 'r').readlines()
        dry_target_people = ''.join(dry_target_people).split('\n')[:-1]
        print("Returing dry:%s"%dry_target_people)
        sys.stdout.flush()
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

@flusher
def send_messages_simple(sk, all_target_people, external_content):
    print("Sending messages simple...")
    for this_id in tqdm.tqdm(all_target_people):
        this_info = "Hi,\n%s"%(external_content)
        print("Now on: %s"%this_id)
        sys.stdout.flush()
        blob, sk = relentlessly_get_blob_by_id(sk, this_id, username, password)
        if blob is None:
            continue
        else:
            max_giveup_chat_by_blob(sk, blob, this_info, username, password, this_id)
            print("Message sent for %s:%s"%(this_id, this_info))
    return


@flusher
def send_messages(sk, pd_blobs, external_content=None):
    print("Sending messages...")
    for row in tqdm.tqdm(range(len(pd_blobs))):
        this_id = pd_blobs.iloc[row].id
        this_name = pd_blobs.name[row]
        this_info = pd_blobs.iloc[row].contents if external_content is None else "Hi %s,\n%s"%(this_name, external_content)
        print("Now on: %s"%this_name)
        sys.stdout.flush()
        blob, sk = relentlessly_get_blob_by_id(sk, this_id, username, password)
        if blob is None:
            continue
        else:
            if str(blob.name) == '':
                continue
            #Pressure test mode:
            n = 0
            while PRESSURE_TEST:
                message = "Pressure test...%s"%n
                max_giveup_chat_by_blob(sk, blob, message, username, password, this_id)
                pd_blobs.chat_times_sent.iat[row] += 1
                n += 1
                print("Message sent for %s:%s"%(this_id, message))
            #Normal mode:
            max_giveup_chat_by_blob(sk, blob, this_info, username, password, this_id)
            pd_blobs.chat_times_sent.iat[row] += 1
            print("Message sent for %s:%s"%(this_id, this_info))
    return

def ignore_old(sk, commander_ids, username, password):
    for commander_id in commander_ids:
        tmp = ['old_messages']
        while len(tmp)>0:
            tmp, sk = relentlessly_get_commander_message(sk, commander_id, username, password)
        print("Old messages with %s ignored."%commander_id)

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
    WAIT_TIME = 35
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
    #username = '18601156335'
    username = 'mengxuan@bancosta.com'
    #password = 'lmw196411'
    password = 'Bcchina2020'
    me_id = "live:a4333d00d55551e"

    additional_contacts_path = 'data/%s/saved_contacts.txt'%username
    remove_contacts_path = 'data/%s/removed_contacts.txt'%username
    template_file = "data/%s/content.txt"%username
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
    commander_ids = ['live:892bfe64f9296876', 'live:mengxuan_9', me_id]
    ignore_old(sk, commander_ids, username, password)
    while 1:
        chat_messages = []
        for commander_id in commander_ids:
            chat_messages, sk = relentlessly_get_commander_message(sk, commander_id, username, password)
        print("%s new messages with commander"%len(chat_messages), chat_messages)
        for message_this in chat_messages:
            if ("TOKEN" in message_this.content) and (message_this.userId in commander_ids):
                daily_report = message_this.content.strip("TOKEN").replace("TOKEN",'')
                print("Token caught")
                #Send for every one:
                #send_messages(sk, pd_blobs, external_content = daily_report)
                send_messages_simple(sk, all_target_people, external_content = daily_report)
                #then empty commanders message:
                ignore_old(sk, commander_ids, username, password)

        time.sleep(5)
   
    sys.exit()
   
    
    
   
