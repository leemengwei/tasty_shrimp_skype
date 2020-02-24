#Skype 版本 8.56.0.100 2019
from skpy import Skype
from IPython import embed
import os,sys
import pandas as pd
import tqdm
import time
import datetime

def login_web_skype(username, password, sleep=0):
    print("Logging in as %s"%username)
    time.sleep(sleep)
    sk = Skype(username, password) # connect to Skype
    return sk

def get_template_to_send(template_file):
    print("Getting template...")
    content = open(template_file, 'r').readlines()
    content = ''.join(content).replace('\n', '\t\n')
    return content

def relentlessly_get_blob_by_id(sk, i):
     #RELENTLESSLY:
     try:
         blob = sk.contacts[i]
     except:
         sk = None
         blob = 'broken_tag'
         while (sk==None) or (blob=='broken_tag'):
             try:
                 sk = login_web_skype(username, password, sleep=0)
                 blob = sk.contacts[i]
             except Exception as e:
                 print(e)
                 pass
     return sk, blob

def check_invalid_account(sk, all_target_people):
    print("Checking invalid account...")
    #check again if valid:
    for idx,i in tqdm.tqdm(enumerate(all_target_people)):
        print(idx)
        sk, blob = relentlessly_get_blob_by_id(sk, i)
        if blob is None:
            print("Removing invalid %s"%i)
            all_target_people.remove(i)
        else:
            pass
    f = open("output/nowadays_account.txt", 'w')
    for i in all_target_people:
        f.write(i)
        f.write("\n")
    f.close()
    return all_target_people

def get_all_target_people(sk):    
    #Get sync contacts
    print("Getting contacts...")
    sk.contacts.sync()
    all_contacts = sk.contacts.contactIds
    #others_contacts = ['maguozhi.kemen']
    with open("output/nowadays_account_%s.txt"%str(datetime.datetime.today()), 'w') as f:
        for i in all_contacts:
            f.write(i+ '\n')
    others_contacts = open("output/nowadays_account.txt", 'r').readlines()
    others_contacts = ''.join(others_contacts).split('\n')[:]
    all_target_people = all_contacts + others_contacts
    #if check valid, could be slow
    #all_target_people = check_invalid_account(sk, all_target_people)
    print("%s people returned"%len(all_target_people))
    return all_target_people

def parse_infos(sk, all_target_people, template_contents):
    print("Parsing infos...")
    data = {'id':all_target_people, 'name':None, 'location':None, 'language':None, 'avatar':None, 'mood':None, 'phones':None, 'birthday':None, 'authorised':None, 'blocked':None, 'favourite':None, "contents":None, "counts":0, 'times':None, 'misc':None}
    pd_blobs = pd.DataFrame(data)
    for each_id in tqdm.tqdm(all_target_people):
        sk, blob = relentlessly_get_blob_by_id(sk, each_id)
        if blob is None:continue
        for its_attr in blob.attrs:
            which_row = pd_blobs[pd_blobs.id==each_id].index
            if its_attr == 'name':
                pd_blobs.loc[which_row, its_attr] = str(eval("blob.%s"%its_attr)).split(' ')[0]
            else:
                pd_blobs.loc[which_row, its_attr] = str(eval("blob.%s"%its_attr))
    return pd_blobs

def send_messages(sk, pd_blobs):
    print("Sending messages...")
    for row in tqdm.tqdm(range(len(pd_blobs))):
        this_id = pd_blobs.iloc[row].id
        this_info = pd_blobs.iloc[row].contents
        sk, ch = relentlessly_get_blob_by_id(sk, this_id)
        if ch is None:continue
        ch = ch.chat
        #ch.sendMsg(this_info)
        pd_blobs.counts.iat[row] += 1
        print("Message sent for %s"%this_id)
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
    print("Starting...")
    template_file = "templates/content.txt"
    #username = '18601156335'
    username = 'mengxuan@bancosta.com'
    #password = 'lmw196411'
    password = 'Bcchina2020'
    
    #Log in:
    sk = login_web_skype(username, password)

    #Get all targets:
    all_target_people = get_all_target_people(sk)
    sys.exit()
 
    #Get templates:
    template_contents = [get_template_to_send(template_file)]*len(all_target_people)

    #Parse together:
    pd_blobs = parse_infos(sk, all_target_people, template_contents)

    #Send for every one:
    send_messages(sk, pd_blobs)
 
   
    sys.exit()
   
    
    
   
