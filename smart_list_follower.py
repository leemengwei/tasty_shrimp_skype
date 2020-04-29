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

username = '18601156335' 
#username = 'mengxuan@bancosta.com' 
password = 'lmw196411' 
#password = 'Bcchina2020' 
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
        self.demand_lists = glob.glob(list_path+"/cargo_*.csv")
        self.demand_lists.sort()
        num_of_demand_lists = len(self.demand_lists)
        if num_of_demand_lists != self.old_num:
            print("%s demand lists for now..."%num_of_demand_lists, self.demand_lists)
            self.old_num = num_of_demand_lists
        
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
                #Cooked or Raw?
                #PIC_status[this_PIC] = True if this_PIC in event.msg.userId and event.msg.content=='cook' else False
                PIC_status[this_PIC] = True if this_PIC in event.msg.userId else False
            #很可能已经有旧的状态表了：
            old_status = None
            this_status_list = list_path+'status_for_%s'%this_demand_list.split('/')[-1]
            if os.path.exists(this_status_list):
                old_status = pd.read_csv(this_status_list, index_col=0)  #读旧
            now_status = pd.DataFrame({'STATUS':list(PIC_status.values())}, index=list(PIC_status.keys()))
            if old_status is not None:
                common_index = list(set(now_status.index) & set(old_status.index))
                now_status.loc[common_index] = old_status.loc[common_index]|now_status.loc[common_index]
            now_status.to_csv(this_status_list)   #存新
            print(this_status_list.split('/')[-1], '\n', now_status, '\n')
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

        #Cook?

if __name__ == "__main__":
    print("Start")
    sk = SkypePing()

    sk.loop()







