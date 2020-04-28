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
username = '18601156335' 
#username = 'mengxuan@bancosta.com' 
password = 'lmw196411' 
#password = 'Bcchina2020' 
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

class SkypePing(SkypeEventLoop):
    def __init__(self):
        print("Now preparing listener...")
        self.reply_status = {}
        self.num_of_bills_old = 0
        self.bills_blob_old = {}
        self.he_who_replied = []
        self.update_bills()
        self.update_reply_status(None)
        #Log in here:
        print("Now logging in...", username, password)
        super(SkypePing, self).__init__(username, password)
        print("Now listenning...")

    def update_bills(self):
        #Thoroughly refresh for bills info each time.
        self.bills_blob = {}
        time.sleep(1)
        self.bills_now = glob.glob(bill_path+"/*.csv")
        num_of_bills_now = len(self.bills_now)
        if num_of_bills_now != self.num_of_bills_old:
            print("%s bills for now..."%num_of_bills_now, self.bills_now)
            self.num_of_bills_old = num_of_bills_now
        #Update content over bills:
        for this_bill in self.bills_now:
             try:
                self.bills_blob[this_bill] = pd.read_csv(this_bill)
             except Exception as e:
                print("Error reading bill,", this_bill)
        #If modification occured:
        if str(self.bills_blob) != str(self.bills_blob_old):
            print("Modification Detected.", datetime.datetime.now())
            self.bills_blob_old = self.bills_blob
            #Immediately re-target PIC:
            self.PICs = []
            for this_bill in self.bills_now:
                self.PICs += list(self.bills_blob[this_bill].PIC_SKYPE)

    def update_reply_status(self, event):
        someone = None
        if isinstance(event, SkypeNewMessageEvent):
            someone = event.msg.userId
            print(someone, 'says:', event.msg.content)
        if someone in self.PICs:
            self.he_who_replied += [someone]
            print("%s replied! Noted"%someone)

    def wake_up_time(self):
        now = datetime.datetime.now()
        if now.second==9:
            #print("Launcher ready, but not now, waiting...")
            return False
        else:
            print("Launching Now! Target:", self.PICs)
            return True

    def onEvent(self, event):
        #I tried start pool outside, failed. Now can only triggered by skype event.
        self.update_bills()
        self.update_reply_status(event)
        
        if self.wake_up_time():
            pool = Pool(processes=4)
            struct_list = [[1],[2],[3]]
            status = pool.map(launch, struct_list)
            pool.close()
            pool.join()

        print("Return Listening...", datetime.datetime.now())


if __name__ == "__main__":
    print("Start")
    sk = SkypePing()

    sk.loop()







