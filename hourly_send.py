import smtplib
import email
from email.mime.text import MIMEText
from email.mime.image import MIMEImage
from email.mime.multipart import MIMEMultipart
from email.header import Header
from IPython import embed
import datetime
import pandas as pd
import daily_bob
import timeout_decorator
from multiprocessing.pool import Pool
import sys
import numpy as np
import time


def ideal_pool_chats_by_blob(struct):
    skype_id, messages, sk = struct[0], struct[1], struct[2]
    @timeout_decorator.timeout(WAIT_TIME)
    def auto_timeout_blob_and_chat(sk, skype_id, messages):
        blob, sk = daily_bob.relentlessly_get_blob_by_id(sk, skype_id, username, password)
        if blob == None and 'live:' not in skype_id:
            print("%s does not exisit, trying live:%s"%(skype_id, skype_id))
            blob, sk = daily_bob.relentlessly_get_blob_by_id(sk, "live:"+skype_id, username, password)
        sys.stdout.flush()
        tmp_len = 1
        history_chats = []
        while tmp_len>0: #check historical messages
            try:   #for whom is not your contacts, getMsg must fail, then skip~
                tmp = blob.chat.getMsgs()
            except:
                tmp = []
            history_chats += tmp
            tmp_len = len(tmp)
        print("*SKYPE* Pool Sending to %s"%skype_id)
        for idx, message in enumerate(messages):
            if message in str(history_chats):
                #print("*SKYPE* Pool Already sent,", idx, skype_id)
                pass
            else:
                try:   #HEHE, force try to push all msgs to he who reports 403.
                    blob.chat.sendMsg(message)
                    time.sleep(0.5)
                except Exception as e:
                    if '403' in str(e):
                        print("Hehe, %s is not your contacts, while I forced to push him messages"%skype_id)
                    else:
                        raise e
        return

    try:
        auto_timeout_blob_and_chat(sk, skype_id, messages)
        print("Okay", skype_id)
        return True
    except Exception as e:
        print("When sending %s,"%skype_id, e)
        sys.stdout.flush()
        return False

def email_send_action(mail_sender, mail_receivers, subject_content, body_content):
    #MAIN:
    mm = MIMEMultipart('related')
    mm["From"] = "<%s>"%mail_sender
    mm["BCC"] = ', '.join(["<%s>"%i for i in mail_receivers])
    mm["Subject"] = Header(subject_content, 'utf-8')
    #content:
    message_text = MIMEText(body_content, "plain", "utf-8")
    mm.attach(message_text)
    
    #attach a jpg
    #image_data = open('data/portrait.jpg', 'rb')
    #message_image = MIMEImage(image_data.read())
    #image_data.close()
    #mm.attach(message_image)

    #log in service:
    stp = smtplib.SMTP()
    stp.connect(mail_host, 25)  
    stp.set_debuglevel(0)
    stp.login(mail_sender,mail_license)

    #send:
    stp.sendmail(mail_sender, mail_receivers, mm.as_string())
    stp.quit()
    return

#def get_email_content(EMAIL_FILE_NAME, this_MV):
#    content = open(EMAIL_FILE_NAME, 'r').readlines()
#    content = ''.join(content).replace('\n', '\t\n')
#    content = subject_content + '\n' + content + '\n' + str(datetime.datetime.now()).split(' ')[0] 
#    return subject_content, content

def get_skype_content(SKYPE_FILE_NAME, this_MV):
    skype_contents = open(SKYPE_FILE_NAME, 'r').readlines()
    skype_contents = ''.join(skype_contents).replace('MV_TOKEN', this_MV).split('LINE_TOKEN')
    for idx,_ in enumerate(skype_contents):
        skype_contents[idx] = _.strip('\n')
    return skype_contents


if __name__ == "__main__":
    print("Auto 126 Emailing and Skyping...")
    
    #CONFIGURATIONS:
    #Email configuration:
    CARGO_FILE_NAME = "data/lists_listener/"+sys.argv[1]
    CARGO_STATUS_FILE_NAME = "data/lists_listener/status_for_"+CARGO_FILE_NAME.split('/')[-1]
    #EMAIL_FILE_NAME = 'data/data_bonding_net/email_content.txt'
    mail_host = "smtp.126.com"
    mail_sender = "limengxuan0708@126.com"
    mail_license = "lmx921221"  #this is not password!
    #Skype configuration:
    WAIT_TIME = 25
    SKYPE_FILE_NAME = 'data/lists_listener/skype_content.txt'
    #username = 'mengxuan@bancosta.com'
    username = '18601156335'
    #password = 'Bcchina2020'
    password = 'lmw196411'
    sk = daily_bob.relentless_login_web_skype(username, password, WAIT_TIME=WAIT_TIME)

    #Get data:
    middle_bond = pd.read_csv(CARGO_FILE_NAME)
    row_MV = {}
    row_MSG = {}
    row_MAILBOXES = {}
    row_SKYPES = {}
    row_PIC = {}
    print("Collecting data...")
    for row_num,i in enumerate(middle_bond.iterrows()):
        i = i[1]
        try:
            mail_receivers = i.MAILBOXES.strip("[]'").replace(' ','').replace("','",",").replace("'","").replace("\"","").split(',')
        except Exception as e:
            print("Skip this row when getting EMAIL for row:%s, %s."%(row_num, i.MV), e)
            mail_receivers = []
        try:
            skypes_receivers = i.SKYPES.strip("[]'").replace(' ','').replace("','",",").replace("'","").replace("\"","").split(',')
        except Exception as e:
            print("Skip this row when getting SKYPES for row:%s, %s."%(row_num, i.MV), e)
            skypes_receivers = []
        try:
            pic_skype_receiver = i.PIC_SKYPE.strip("[]'").replace(' ','').replace("','",",").replace("'","").replace("\"","").split(',')
        except Exception as e:
            print("Skip this row when getting PIC for row:%s, %s."%(row_num, i.MV), e)
            pic_skype_receiver = []
        try:
            mail_receivers.remove('')
            skypes_receivers.remove('')
            pic_skype_receiver.remove('')
        except:
            pass
        row_MV[row_num] = i.MV
        row_MAILBOXES[row_num] = mail_receivers
        row_SKYPES[row_num] = skypes_receivers
        row_PIC[row_num] = pic_skype_receiver

    #1) Skyping:
    #Form chats content
    #try:
    #    status_file_content = pd.read_csv(CARGO_STATUS_FILE_NAME, index_col=0)
    #except Exception as e:
    #    print("Error reading statu file %s, this file is required before sending anything in lastest version. EXITING!!"%CARGO_STATUS_FILE_NAME)
    #    sys.exit()
    for row_num in row_MV.keys():
        this_MV = row_MV[row_num]
        row_MSG[row_num] = get_skype_content(SKYPE_FILE_NAME, this_MV)
    struct_list = []
    for row_num in row_PIC.keys():   #keys are 0123...
        for this_PIC in row_PIC[row_num]:
            if len(this_PIC)>0:
                #if status_file_content.STATUS.loc[this_PIC] == True:   #He's been replied
                #    print("Pass %s, his MV replied"%this_PIC)           #won't send him message again
                #else:
                struct_list.append([this_PIC, row_MSG[row_num], sk, row_num, row_MV[row_num]])

    #Skype send action:
    print("--------NOW SKYPE--------")
    n = 0
    pool = Pool(processes=4)
    failed_pic = []
    failed_rows = []
    failed_vessels = []
    while n<6 and len(struct_list)>0:
        if n == 0:
            pass
        else:
            sk = daily_bob.relentless_login_web_skype(username, password)
            sk.conn.verifyToken(sk.conn.tokens)
        status = pool.map(ideal_pool_chats_by_blob, struct_list)
        struct_list = np.array(struct_list)[np.where(np.array(status)==False)].tolist()
        if len(struct_list)>0:
            failed_pic = np.array(struct_list)[:,0].tolist()
            failed_rows = np.array(struct_list)[:,3].tolist()
            failed_vessels = np.array(struct_list)[:,4].tolist()
            print("Retrying(%s):"%n, failed_vessels)
            time.sleep(5)
            for _struct_ in struct_list:
                _struct_[2] = sk      #Renew sk for pools after login
        n += 1
    pool.close()
    pool.join()

    #Report:
    print("\n*********Final Failures*********: (check manually, and consider run again with them)\n")
    for i in range(len(failed_rows)):
        print("row:", failed_rows[i], "vessel:", failed_vessels[i], "pic_skype:", failed_pic[i])

    #2) Emailing for failures:
    print("--------NOW EMAIL FOR FAILED--------")
    for row_num,i in enumerate(middle_bond.iterrows()):
        this_MV = row_MV[row_num]
        if this_MV not in failed_vessels:continue
        mail_receivers = row_MAILBOXES[row_num]
        if len(mail_receivers) == 0:
            pass
        else:
            subject_content = 'MV %s/Suitable cargo - Bancsota desk'%this_MV
            body_content = '\n'.join(get_skype_content(SKYPE_FILE_NAME, this_MV))
            print("*EMAIL* MV %s, Sending to %s"%(this_MV, mail_receivers))
            email_send_action(mail_sender, mail_receivers, subject_content, body_content)

    print("All done!")
    #embed()
    
