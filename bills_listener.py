from IPython import embed
import time
import tqdm
import daily_bob
import skpy
import os,sys,glob
import numpy as np
from skpy import SkypeEventLoop, SkypeNewMessageEvent
import pandas as pd
import datetime
import timeout_decorator
from multiprocessing.pool import Pool
import collections

WAIT_TIME = 55 
PRESSURE_TEST = False 
#PRESSURE_TEST = True 
CHECK_CONTACTS_VALID = False 
#CHECK_CONTACTS_VALID = True 
PARSE_FROM_ZERO = False 
#PARSE_FROM_ZERO = True 
DRY_RUN = False 
#DRY_RUN = True 
RESTART_AT = 0 
if PRESSURE_TEST: 
    DRY_RUN = True 
username = '18601156335' 
#username = 'mengxuan@bancosta.com' 
password = 'lmw196411' 
#password = 'Bcchina2020' 
me_id = "live:a4333d00d55551e" 

additional_contacts_path = 'data/%s/saved_contacts'%username 
remove_contacts_path = 'data/%s/removed_contacts'%username 
template_file = "data/%s/content"%username                   

list_path = 'data/lists_listener/'

def launch(struct_list):
    this = struct_list
    print("Bombbing!", this)
    time.sleep(10)
    return True

class SkypePing(SkypeEventLoop):
    def __init__(self):
        print("Now preparing listener...")
        self.reply_status = collections.OrderedDict()
        self.old_num = 0
        self.lists_blob_old = collections.OrderedDict()
        self.he_who_replied = []
        self.PIC_status = collections.OrderedDict()
        #Log in here:
        print("Now logging in...", username, password)
        super(SkypePing, self).__init__(username, password)
        print("Now listenning...")

    def check_demand_lists_and_update_their_status_lists(self, event):
        #看看demand lists：
        self.demand_lists = glob.glob(list_path+"/cargo*.csv")
        self.demand_lists.sort()
        #num_of_demand_lists = len(self.demand_lists)
        #if num_of_demand_lists != self.old_num:
        #    print("%s lists for now..."%num_of_demand_lists, self.demand_lists)
        #    self.old_num = num_of_demand_lists
        
        #生成/更新对应的status lists:
        #如果不是msg事件，直接返回：
        if not isinstance(event, SkypeNewMessageEvent):
            return
        #如果是msg事件，更新一下所有lists的所有PIC的状态：
        print('Chat event...')
        for this_demand_list in self.demand_lists:
            PICs = pd.read_csv(this_demand_list).PIC_SKYPE
            PIC_status = collections.OrderedDict()
            for this_PIC in PICs:
                if not isinstance(this_PIC, str):continue   #试图跳过不可见的空pic
                PIC_status[this_PIC] = True if this_PIC in event.msg.userId and event.msg.content=='cook' else False
            #很可能已经有旧的状态表了：
            old_status = None
            this_status_list = list_path+'status_for_%s'%this_demand_list.split('/')[-1]
            if os.path.exists(this_status_list):
                old_status = pd.read_csv(this_status_list, index_col=0)  #读旧
            now_status = pd.DataFrame({'STATUS':list(PIC_status.values())}, index=list(PIC_status.keys()))
            if old_status is not None:
                now_status = now_status + old_status
            now_status.to_csv(this_status_list)   #存新
            print(this_status_list, '(old)\n', old_status)
            print(this_status_list, '\n', now_status)
        #embed()

    def update_reply_status(self, event):
        someone = None
        if isinstance(event, SkypeNewMessageEvent):
            someone = event.msg.userId
            print(someone, 'says:', event.msg.content)
        if someone in self.PICs:
            self.he_who_replied += [someone]
            print("%s replied! Noted"%someone)

    def cook_time(self):
        now = datetime.datetime.now()
        if now.second!=9:
            #print("Launcher ready, but not now, waiting...")
            return False
        else:
            print("Launching Now! Target:", self.PICs)
            return True

    def onEvent(self, event):
        #有消息才会激活事件，根据目前挂的单更新对应的状态单
        self.check_demand_lists_and_update_their_status_lists(event)
        print("Listening...", datetime.datetime.now())


if __name__ == "__main__":
    print("Start")
    sk = SkypePing()

    sk.loop()







