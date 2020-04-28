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
#username = '18601156335' 
username = 'mengxuan@bancosta.com' 
#password = 'lmw196411' 
password = 'Bcchina2020' 
me_id = "live:a4333d00d55551e" 

additional_contacts_path = 'data/%s/saved_contacts'%username 
remove_contacts_path = 'data/%s/removed_contacts'%username 
template_file = "data/%s/content"%username                   

bill_path = 'data/bills_listener/'

def launch(struct_list):
    this = struct_list[0]
    print("Bombbing!", this)
    time.sleep(0.3)
    return True

#class SkypePing(SkypeEventLoop):
class SkypePing(object):
    def __init__(self):
        print("Now preparing listener...")
        self.update_bills()
        #Log in here:
        print("Now logging in...")
        #super(SkypePing, self).__init__(username, password)
        print("Now listenning...")

    def update_bills(self):
        bills_blob = {}
        bills_now = glob.glob(bill_path+"/*.csv")
        if len(bills_now)==0:
            print("No bills for now...")
        else:
            for this_bill in bills_now:
                try:
                    bills_blob[this_bill] = pd.read_csv(this_bill)
                except Exception:
                    print("Error reading bill,", this_bill)
        return bills_blob

    def wake_up_time(self):
        now = datetime.datetime.now()
        if now.second%5!=0:
            print("Launcher ready, but not now, waiting...")
            return False
        else:
            print("Launching Now!!!")
            return True

    #def onEvent(self, event):
    def onEvent(self):
        while 1:
            time.sleep(1)
            bills = self.update_bills()
            #if isinstance(event, SkypeNewMessageEvent):
            #    print(event.msg.userId, 'says:', event.msg.content)
            
            if self.wake_up_time():
                print("Sender has been woke up...")
                pool = Pool(processes=4)
                struct_list = [[1],[2],[3],[4],[5],[6],[7],[8],[9],[10]]
                status = pool.map(launch, struct_list)
                pool.close()
                pool.join()


if __name__ == "__main__":
    print("Start")
    sk = SkypePing()
    sk.onEvent()








