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
import random

username = '18601156335' 
#username = 'mengxuan@bancosta.com' 
password = 'lmw196411' 
#password = 'Bcchina2020' 
list_path = 'data/lists_listener/'


@timeout_decorator.timeout(10)
def timeout_getblob(sk, to_whom):
    blob = sk.contacts[to_whom]
    return blob

@timeout_decorator.timeout(5)
def timeout_sendMsg(blob, talking_what):
    blob.chat.sendMsg(talking_what)

class SkypePing(SkypeEventLoop):
    def __init__(self):
        print("Now preparing listener...")
        self.column_order_list = ['talking_what', 'when', 'interval']
        self.tasks = pd.DataFrame(columns = self.column_order_list)
        self.when_column_index = np.where('when' == np.array(list(self.tasks)))[0][0]
        self.talking_what_column_index = np.where('talking_what' == np.array(list(self.tasks)))[0][0]
        self.reply_status = collections.OrderedDict()
        self.old_num = 0
        self.lists_blob_old = collections.OrderedDict()
        self.he_who_replied = []
        self.PIC_replied_status = collections.OrderedDict()
        if len(sys.argv) > 1:
            self.tasks = pd.read_csv('data/lists_listener/follow_up_checkpoint.csv', index_col=0) 
            for row_idx,row in enumerate(self.tasks.iterrows()):
                self.tasks.iloc[row_idx, self.when_column_index] = datetime.datetime.strptime(row[1].when.split('.')[0], "%Y-%m-%d %H:%M:%S")
                self.tasks.iloc[row_idx, self.talking_what_column_index] = str(row[1].talking_what)
            print("CHECKPOINT LOADED...")
        self.tasks = self.tasks.sort_index()
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
            MVs = pd.read_csv(this_demand_list).MV
            PICs = pd.read_csv(this_demand_list).PIC_SKYPE
            PIC_replied_status = collections.OrderedDict()
            MV_in_charge_of = collections.OrderedDict()
            for row_idx,this_row_PIC in enumerate(PICs):
                PICs_in_this_box = this_row_PIC.strip("[]'").replace(' ','').replace("','",",").replace("'","").replace("\"","").split(',')
                for this_PIC in PICs_in_this_box:
                    if not isinstance(this_PIC, str):continue   #试图跳过不可见的空pic
                    #Cooked or Raw? 回复的人就记成True，随后关联该船的人也记True
                    PIC_replied_status[this_PIC] = True if this_PIC in event.msg.userId else False
                    MV_in_charge_of[this_PIC] = MVs[row_idx]
            now_status = pd.DataFrame({'STATUS':list(PIC_replied_status.values()), 'MV':list(MV_in_charge_of.values())}, index=list(PIC_replied_status.keys()))
            #随后关联该船的人也记True:
            replied_MV = list(now_status[now_status.STATUS==True].MV)
            for row in now_status.iterrows():
                if row[1].MV in replied_MV:
                    now_status.loc[(row[0],'STATUS')]=True
            #很可能已经有旧的状态表了：
            old_status = None
            this_status_list = list_path+'status_for_%s'%this_demand_list.split('/')[-1]
            if os.path.exists(this_status_list):
                old_status = pd.read_csv(this_status_list, index_col=0)  #读旧
            if old_status is not None:
                common_index = list(set(now_status.index) & set(old_status.index)) #取出common的人，别的不要了
                now_status.loc[(common_index,'STATUS')] = old_status.loc[(common_index,'STATUS')]|now_status.loc[(common_index,'STATUS')]
            now_status.to_csv(this_status_list)   #存新
            print(this_status_list.split('/')[-1], '\n', now_status, '\n')

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
    
    def launch_or_wait(self, event):
        if not isinstance(event, SkypeNewMessageEvent):
            return
        print("Bombbing!")
        logname = '-'.join(str(datetime.datetime.now()).replace(':','').split(' ')).split('.')[0]
        print("python hourly_send.py cargo_1.csv >& %s.log &"%logname)
        os.system("python hourly_send.py cargo_1.csv >& %s.log &"%logname)
        return

    def onEvent(self, event):
        #挂单项目：有消息才会激活事件，根据目前挂的单更新对应的状态单
        #self.check_demand_lists_and_update_their_status_lists(event)
        #self.launch_or_wait(event)

        #监听follow项目：
        #1）先listen：
        if isinstance(event, SkypeNewMessageEvent):
            whos_talking = event.msg.userId
            talking_what = event.msg.content
            to_whom = event.msg.chatId.strip('8:')
            when = event.msg.time
            if not isinstance(talking_what,str):return
            #Case 0 收到测试活动信号
            if whos_talking in ['live:mengxuan_9', 'live:a4333d00d55551e'] and 'Alive?'==talking_what:
                try:
                    blob = timeout_getblob(self.skype, to_whom)
                    timeout_sendMsg(blob, 'Alive-Yes')
                except Exception as e:
                    print("Time out testing...", e)
            #Case 1 收到重复信号
            if whos_talking in ['live:mengxuan_9', 'live:a4333d00d55551e'] and ' .' in talking_what:
                talking_what = talking_what.replace(' .', '')
                try:
                    interval = talking_what[-1]
                    interval = float(interval)*random.uniform(59,62)
                    interval = 0.1 if interval == 0 else interval
                    talking_what = talking_what[:-1]
                    #if talking_what
                except:
                    print("No time interval, pass...")
                    return
                print("Repeating Signal at %s, %s says %s, interval: %s min."%(to_whom, whos_talking, talking_what, interval))
                this_task = pd.DataFrame(index=[to_whom], data = {'talking_what':[talking_what], 'when':[when+datetime.timedelta(minutes=interval+8*60)], 'interval':[interval]}, columns=self.column_order_list)
                #任务更新操作：任务从始至终叠加
                self.tasks = self.tasks.append(this_task)
                self.tasks = self.tasks.sort_index()
                print(self.tasks)
            #Case 2 收到停止信号
            elif (whos_talking in ['live:mengxuan_9', 'live:a4333d00d55551e'] and ' ~' in talking_what) or (whos_talking in self.tasks.index):
                print("Canceling Signal at %s"%to_whom)
                if to_whom in self.tasks.index:
                    self.tasks = self.tasks.drop(to_whom)
                else:
                    print("No task for %s"%to_whom)
                print(self.tasks)
            #Case 3 没收到有效信号
            else:
                pass
            #blob, sk = daily_bob.relentlessly_get_blob_by_id(self.skype, to_whom, username, password)
            self.tasks[self.column_order_list].to_csv('data/lists_listener/follow_up_checkpoint.csv') 
        else:    #其他事件

            pass

        #2）再follow：对于超时未回复的，自动Follow up
        to_whom_old = None
        for row_idx,row in enumerate(self.tasks.iterrows()):
            to_whom = row[0]
            talking_what = row[1].talking_what
            if row[1].when<datetime.datetime.now():
                if not isinstance(talking_what, str) or len(talking_what)==0:continue
                print("Now following....", to_whom, talking_what)
                try:
                    if to_whom!=to_whom_old:
                        blob = timeout_getblob(self.skype, to_whom)
                    timeout_sendMsg(blob, talking_what)
                    #把follow完的自动更新一下任务状态
                    self.tasks.iloc[row_idx, self.when_column_index] = datetime.datetime.now()+datetime.timedelta(minutes=row[1].interval)
                    to_whom_old = to_whom
                except Exception as e:
                    if '403' in str(e):
                        self.tasks.iloc[row_idx, self.when_column_index] = datetime.datetime.now()+datetime.timedelta(minutes=row[1].interval)
                        print("403 Sending %s, as success"%to_whom)
                    else:
                        print("Error sending %s, will retry soon..."%to_whom, talking_what, e)
                        pass
        print("Event type:", type(event), int(random.uniform(10000,20000)))


if __name__ == "__main__":
    print("Start")
    sk = SkypePing()

    sk.loop()







