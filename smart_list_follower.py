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
import re
import add_ons
import copy

#SETTINGS:
USERNAME = '1099366685@qq.com' 
#USERNAME = 'mengxuan@bancosta.com' 
PASSWORD = 'lmw196411' 
#PASSWORD = 'Bcchina2020' 
list_path = 'data/lists_listener/'
MAX_SEND = 7    #最大重复次数

if USERNAME == '1099366685@qq.com':   #off line test case
    COMMANDERS = {'test_mengxuan':'live:892bfe64f9296876', '1099366685@qq.com':'live:1099366685'}
else:             #real case
    COMMANDERS = {'mengxuan@bancosta.com':'live:mengxuan_9', '18601156335':'live:a4333d00d55551e'}

def my_print(what, a='',b='',c='',d='',e='',f='',g=''):
    print(what,a,b,c,d,e,f,g)
    sys.stdout.flush()
    return

@timeout_decorator.timeout(30)
def timeout_getblob(sk, to_whom):
    blob = sk.contacts[to_whom]
    return blob

@timeout_decorator.timeout(20)
def timeout_sendMsg(blob, talking_what):
    blob.chat.sendMsg(talking_what)
    pass

class SkypePing(SkypeEventLoop):
    def __init__(self):
        #Log in here:
        global report_to
        my_print("Now logging in... and reporting to %s Are You Sure????"%(report_to),USERNAME, PASSWORD)
        #input()
        super(SkypePing, self).__init__(USERNAME, PASSWORD)
        my_print("Now preparing listener...")
        self.column_order_list = ['talking_what', 'when', 'interval', 'counter', 'dirty']
        self.tasks = pd.DataFrame(columns = self.column_order_list)
        self.when_column_index = np.where('when' == np.array(list(self.tasks)))[0][0]
        self.interval_column_index = np.where('interval' == np.array(list(self.tasks)))[0][0]
        self.talking_what_column_index = np.where('talking_what' == np.array(list(self.tasks)))[0][0]
        self.counter_column_index = np.where('counter' == np.array(list(self.tasks)))[0][0]
        self.reply_status = collections.OrderedDict()
        self.old_num = 0
        self.lists_blob_old = collections.OrderedDict()
        self.he_who_replied = []
        self.PIC_replied_status = collections.OrderedDict()
        self.is_ready = False

        #无忧Restart相关事宜
        if len(sys.argv) > 1:
            self.tasks = pd.read_csv('data/lists_listener/follow_up_checkpoint.csv', index_col=0) 
            for row_idx,row in enumerate(self.tasks.iterrows()):
                self.tasks.iloc[row_idx, self.when_column_index] = datetime.datetime.strptime(row[1].when.split('.')[0], "%Y-%m-%d %H:%M:%S")
                self.tasks.iloc[row_idx, self.talking_what_column_index] = str(row[1].talking_what)
            my_print("CHECKPOINT LOADED...CHECKING HISTORY.... ")
            #所有无忧启动,check历史聊天,形成dict,保存下来
            history_chats_dict = {}
            check_who_old = ''
            for row in self.tasks.iterrows():
                check_who = row[0]
                my_print("Reading history chats %s"%check_who)
                if row[1].interval == 24*60 or check_who == check_who_old:continue  #连续的人 就不check了
                blob = "__"
                while 1:
                    try:
                        blob = timeout_getblob(self.skype, check_who)
                    except:
                        super(SkypePing, self).__init__(USERNAME, PASSWORD)
                    if blob != '__':break
                history_chats = []
                for i in range(4):
                    try:
                        history_chats += blob.chat.getMsgs()  #聊天不够有可能导致失败
                    except:
                        history_chats += ' '
                history_chats_dict[check_who] = history_chats
                check_who_old = check_who
            #如果历史中他已经在这条消息下边出现过回复了，则pass,且修订下次为一天后,interval也要改
            for row_idx,row in enumerate(self.tasks.iterrows()):
                if self.tasks.iloc[row_idx, self.interval_column_index] == 24*60:continue#间隔都24h了，说明他被check过了
                check_who = row[0]
                talking_what = row[1].talking_what
                he_talked_at = [999999]
                I_talked_at = [99990]
                for idx,tmp in enumerate(history_chats_dict[check_who]):
                    if isinstance(tmp, str):continue   #如果是str类型，则是之前读取history失败随便填充的
                    if tmp.userId == check_who:
                        he_talked_at.append(idx)
                    if talking_what in tmp.content:
                        I_talked_at.append(idx)
                if min(he_talked_at) > min(I_talked_at):
                    my_print("No fucking reply, will just sent %s latter!"%check_who)
                    continue    #fuck him, will just sent him latter!
                if min(he_talked_at) < min(I_talked_at):     #he saw my message!
                    self.tasks.iloc[row_idx, self.when_column_index] = datetime.datetime.now()+datetime.timedelta(minutes=24*60)
                    self.tasks.iloc[row_idx, self.interval_column_index] = 24*60
                    my_print("%s has been replied to msg:%s..., will send after 24h"%(check_who,talking_what[:20]))
        self.tasks = self.tasks.sort_index()
        self.tasks[self.column_order_list].to_csv('data/lists_listener/follow_up_checkpoint.csv') 
        my_print("Now listenning...")

    def leave_traces_before_drop(self):
        my_print("Leaving traces %s"%self.for_traces)
        self.for_traces = pd.DataFrame(self.for_traces).T if not isinstance(self.for_traces, pd.core.frame.DataFrame) else self.for_traces
        self.for_traces.to_csv('data/lists_listener/follow_traces.csv', mode='a', header=False)
        return

    def listen_method(self, event):
        whos_talking = event.msg.userId
        talking_what = event.msg.content
        to_whom = event.msg.chatId.replace("8:", '')
        when = event.msg.time
        if not isinstance(talking_what,str):return
        #Case 1 收到新任务 .x
        if whos_talking in COMMANDERS.values() and ' .' in talking_what:  #' .' or ' ..'
            try:
                interval = talking_what[-1]
                scaler = 10 if ' ..' in talking_what else 1
                interval = float(interval)*random.uniform(50,70)*scaler
                interval = 0.1 if interval == 0 else interval
                talking_what = talking_what.replace(' .', '')
                talking_what = talking_what[:-1]
                #if talking_what
            except:
                my_print("No time interval, pass...")
                return
            this_task = pd.DataFrame(index=[to_whom], data = {'talking_what':[talking_what], 'when':[when+datetime.timedelta(minutes=interval+8*60)], 'interval':[interval], "counter":1, "dirty":False}, columns=self.column_order_list)
            #Interfere任务更新操作：任务从始至终叠加
            self.tasks = self.tasks.append(this_task)
            self.tasks = self.tasks.sort_index()
        #Case 2 收到单人全停信号 ~
        if (whos_talking in COMMANDERS.values() and ' ~' in talking_what):  
            if to_whom in self.tasks.index:
                #Interfere:
                self.for_traces = copy.deepcopy(self.tasks.loc[to_whom])
                if len(self.for_traces)>0:
                    self.leave_traces_before_drop()
                self.tasks = self.tasks.drop(to_whom)
                try:
                    timeout_sendMsg(REPORT_BLOB, "Job dropped at: "+to_whom)    #Report to Mowin
                    timeout_sendMsg(timeout_getblob(to_whom), "~")    #Report to him as well
                except Exception as e:
                    my_print(e)
        #Case 3 收到相关人回复 
        #某人回复了，则所有关于他的聊天24h或更长之后repeat
        if whos_talking in self.tasks.index:
            #Interfere:
            self.tasks.loc[whos_talking, 'when'] = datetime.datetime.now()+datetime.timedelta(minutes=24*60)
            self.tasks.loc[whos_talking, 'interval'] = 24*60
        #Case 4 收到消单信号 [CANCEL:xxx][WHOM:xxx]
        elif whos_talking in COMMANDERS.values():
            cancel_what = re.findall("\[CANCEL: (.*?)\]", talking_what)
            cancel_whom = re.findall("\[WHOM: (.*?)\]", talking_what)
            if len(cancel_what)<=0:
                pass  #连取消什么都没给，直接跳过
            else:
                cancel_what = cancel_what[0]
                cancel_whom = cancel_whom[0] if len(cancel_whom)>0 else 'IM FUCKED'  #Im fucked 是决不会出现的id
                #一行行看内容、id，决定是否keep：
                idx_keep = []
                for idx,i in enumerate(self.tasks.iterrows()):
                    if cancel_what not in i[1].talking_what:
                        idx_keep.append(idx)
                    else:   # 出现了cancel涉及的内容
                        if cancel_whom == 'IM FUCKED':   #Subcase1 没声明谁，整批量取消
                            try:
                                timeout_sendMsg(REPORT_BLOB, "A job Canceled at: "+i[0])
                            except Exception as e:
                                my_print(e)
                        else:                            #Subcase2 声明是谁了
                            if cancel_whom == i[0]:  # whom也完全匹配
                                try:
                                    timeout_sendMsg(REPORT_BLOB, "A Specific job Canceled for: "+i[0])
                                except Exception as e:
                                    my_print(e)
                            else:                   #whom 没匹配上，那就留着
                                idx_keep.append(idx)
                #Interfere:
                self.for_traces = self.tasks.iloc[list(set(range(len(self.tasks)))-set(idx_keep))]
                if len(self.for_traces)>0:
                    self.leave_traces_before_drop()
                self.tasks = self.tasks.iloc[idx_keep]
        #Case x 收到commander的快速Repeat信号，repeat某些货
        if whos_talking in COMMANDERS.values():
            repeat_what = re.findall("\[REPEAT: (.*?)\]", talking_what)
            if len(repeat_what) <=0:
                #没说repeat什么，跳过
                pass
            else:
                repeat_what = repeat_what[0]
                #一行行看内容、id，决定是否repeat：
                for idx,i in enumerate(self.tasks.iterrows()):
                    if repeat_what not in i[1].talking_what:
                        pass     #这一行内容上没出现需要repeat的内容
                    else:
                        try:
                            timeout_sendMsg(REPORT_BLOB, "Will soon repeating job for %s (%s...)"%(i[0], i[1].talking_what[:10]))
                            #Interfere:
                            self.tasks.iloc[idx, np.where(self.tasks.columns=='when')[0][0]] = datetime.datetime.now()
                            self.tasks.iloc[idx, np.where(self.tasks.columns=='interval')[0][0]] = 24*60
                        except Exception as e:
                            my_print(e)
        #Case 5 收到测试活动信号 [TASKS]
        if whos_talking in COMMANDERS.values() and '[TASKS]'==talking_what:
            try:
                timeout_sendMsg(REPORT_BLOB, str(self.tasks))
            except Exception as e:
                my_print("Time out testing...", e)
        #Case 5 收到测试活动信号 [STATE]
        if whos_talking in COMMANDERS.values() and '[STATE]'==talking_what:
            try:
                timeout_sendMsg(REPORT_BLOB, 'Alive-True, Ready-%s'%self.is_ready)
            except Exception as e:
                my_print("Time out testing...", e)
        #Case 6 收到READY信号 [READY]
        if whos_talking in COMMANDERS.values() and '[READY]'==talking_what:
            self.is_ready = True
            try:
                timeout_sendMsg(REPORT_BLOB, 'Ready-%s'%self.is_ready)
            except Exception as e:
                my_print("Time out testing...", e)
        #Case 7 收到暂停信号 [PAUSE]
        if whos_talking in COMMANDERS.values() and '[PAUSE]'==talking_what:
            self.is_ready = False
            try:
                timeout_sendMsg(REPORT_BLOB, 'Ready-%s'%self.is_ready)
            except Exception as e:
                my_print("Time out testing...", e)
        #Case 8 收到调试信号 [EMBED]
        if whos_talking in COMMANDERS.values() and '[*EMBED*]'==talking_what:
            embed()
        #Case 9 收到退出信号 [EXIT]
        if whos_talking in COMMANDERS.values() and '[*EXIT*]'==talking_what:
            sys.exit()
        pass
        #blob, sk = daily_bob.relentlessly_get_blob_by_id(self.skype, to_whom, USERNAME, PASSWORD)
        #Disk:
        self.tasks[self.column_order_list].to_csv('data/lists_listener/follow_up_checkpoint.csv') 
        return

    def follow_method(self):
        #对于超时未回复的，自动Follow up
        to_whom_old = None
        enough_is_enough = []
        for row_idx,row in enumerate(self.tasks.iterrows()):
            to_whom = row[0]
            talking_what = row[1].talking_what
            if row[1].when<datetime.datetime.now():    #某人该发了
                if not isinstance(talking_what, str) or len(talking_what)==0:continue
                my_print("Now following....", to_whom, talking_what)
                if to_whom!=to_whom_old:   #如果两个连续的是同一个人，那么不用重复getblob了
                    try:
                        blob = timeout_getblob(self.skype, to_whom)
                    except Exception as e:
                        blob = []
                        my_print("Error getting blob %s, will retry soon..."%to_whom)
                if row[1].interval != 24*60:    #分为用24h的语料和非24h的
                    sampled_add_ons = random.choice(add_ons.add_ons_not_24[row[1].counter]) if len(add_ons.add_ons_not_24[row[1].counter])>0 else []
                else:
                    sampled_add_ons = random.choice(add_ons.add_ons_24[row[1].counter]) if len(add_ons.add_ons_24[row[1].counter])>0 else []
                if len(sampled_add_ons)==0:  #没有addons，应该刚发没几次，直接发就可以了。
                    try:
                        timeout_sendMsg(blob, talking_what)
                    except Exception as e:
                        if '403' in str(e):
                            my_print("403 Sending %s, as success"%to_whom)
                        else:
                            my_print("Error sending %s, will retry soon..."%to_whom, talking_what, e)
                            pass
                else:  #有add_ons:
                    for _i_ in sampled_add_ons:
                        _i_ = talking_what if _i_ == 'SKYPE_CONTENT' else _i_
                        _i_ = _i_.replace("SKYPE_CONTENT", talking_what)
                        try:
                            timeout_sendMsg(blob, _i_)
                        except Exception as e:
                            if '403' in str(e):
                                my_print("403 Sending %s, as success"%to_whom)
                            else:
                                my_print("Error sending %s, will retry soon..."%to_whom, talking_what, e)
                                pass
                #把follow完的自动更新一下任务状态
                self.tasks.iloc[row_idx, self.when_column_index] = datetime.datetime.now()+datetime.timedelta(minutes=row[1].interval)
                self.tasks.iloc[row_idx, self.counter_column_index] += 1
                if self.tasks.iloc[row_idx, self.counter_column_index] > MAX_SEND:
                    enough_is_enough.append(row_idx)
                    my_print("Enough for %s, will sooner let him go"%to_whom)
                else:
                    pass
                to_whom_old = to_whom
            else:       #还不到时间
                pass
        #Interfere,发的够多的就可以不要了，少的留着
        self.for_traces = copy.deepcopy(self.tasks.iloc[enough_is_enough])
        if len(self.for_traces)>0:
            self.leave_traces_before_drop()
        self.tasks = self.tasks.iloc[list(set(range(len(self.tasks.index)))-set(enough_is_enough))]
        self.tasks[self.column_order_list].to_csv('data/lists_listener/follow_up_checkpoint.csv')
        

    def onEvent(self, event):
        #挂单项目：有消息才会激活事件，根据目前挂的单更新对应的状态单
        #self.check_demand_lists_and_update_their_status_lists(event)
        #self.launch_or_wait(event)

        #监听follow项目：
        #1）如何listen：
        if isinstance(event, SkypeNewMessageEvent):
            self.listen_method(event)
            #my_print(self.tasks)

        #2）如何follow：
        if self.is_ready:
            self.follow_method()
        else:
            pass
        my_print("Event type:", type(event), datetime.datetime.now())


if __name__ == "__main__":
    my_print("Start")
    report_to = COMMANDERS[list(set(COMMANDERS.keys()) - set([USERNAME]))[0]]
    sk = SkypePing()
    print("Reporting to :", report_to)
    REPORT_BLOB = timeout_getblob(sk.skype, report_to)
    sk.loop()



