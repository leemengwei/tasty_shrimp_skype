from skpy import Skype
from IPython import embed
import os,sys
import pandas as pd
import tqdm

def login_web_skype(username, password):
    print("Logging in as %s"%username)
    sk = Skype(username, password) # connect to Skype
    return sk

def info_to_send(info_file):
    content = open(info_file, 'r').readlines()
    content = ''.join(content).replace('\n', '\t\n')
    return content

def output_infos():
    for i in tqdm.tqdm(sk.contacts.contactIds):
        f.write("******%s******"%i)
        tmp= sk.contacts.contact(i)
        f.write(tmp.name.first + tmp.name.last)
        f.write(tmp.location.city + tmp.location.region + tmp.location.country)
        f.write(tmp.birthday.ctime())
        f.write("\n")
    for i in sk.contacts.contactIds:
        print(sk.contacts.contact(i))
 
def get_all_target_people():    
    #Get sync contacts
    print("Getting contacts...")
    sk.contacts.sync()
    all_contacts = sk.contacts.contactIds
    #others_contacts = ['maguozhi.kemen']
    others_contacts = []
    all_target_people = all_contacts + others_contacts
    return all_target_people

def parse_infos(all_target_people, all_contents):
    data = {"user_ids": all_target_people, "contents": all_contents, "SENT_FLAGS": False}
    pd_blobs = pd.DataFrame(data)
    return pd_blobs

def sent_messages(pd_blobs):
    for row in tqdm.tqdm(range(len(pd_blobs))):
        this_id = pd_blobs.iloc[row].user_ids
        this_info = pd_blobs.iloc[row].contents
        ch = sk.contacts[this_id].chat
        ch.sendMsg(this_info)
        pd_blobs.SENT_FLAGS.iat[row] = True
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

    #ch = sk.contacts.contact(person_talk_to['id'])
    #ch = sk.contacts.search('8613120090157')
    #ch = sk.contacts.user(person_talk_to['id'])
    
    #ch = sk.chats.create([person_talk_to]) # new group conversation
    #ch = sk.contacts[person_talk_to].chat # 1-to-1 conversation

    #ch.sendMsg("Hi")
    #ch.sendFile(open("song.mp3", "rb"), "song.mp3") # file upload
    #ch.sendContact(sk.contacts["daisy.5"]) # contact sharing

    #ch.getMsgs()
    return

if __name__ == "__main__":
    print("Starting...")
    info_file = "infos/content.txt"
    username = '18601156335'
    #username = 'mengxuan@bancosta.com'
    password = 'lmw196411'
    #password = 'Lmx921221'
    
    #Log in:
    sk = login_web_skype(username, password)

    #Get all targets:
    all_target_people = get_all_target_people()
 
    #Get infos:
    all_contents = [info_to_send(info_file)]*len(all_target_people)

    #Parse together:
    pd_blobs = parse_infos(all_target_people, all_contents)

    #Send for every one:
    sent_messages(pd_blobs)
 
   
    sys.exit()
   
    
    
   
