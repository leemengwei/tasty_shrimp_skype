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

def send_action(mail_sender, mail_receivers, subject_content, body_content):
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
    #embed()
    #stp.sendmail(mail_sender, mail_receivers, mm.as_string())
    stp.quit()
    return

def get_middle_bond(MIDDLE_FILE_NAME):
    data = pd.read_csv(MIDDLE_FILE_NAME)
    return data

def get_email_content(EMAIL_FILE_NAME, this_MV):
    subject_content = 'MV %s/Suitable cargo - Bancsota desk'%this_MV
    content = open(EMAIL_FILE_NAME, 'r').readlines()
    content = ''.join(content).replace('\n', '\t\n')
    content = subject_content + '\n' + content + '\n' + str(datetime.datetime.now()).split(' ')[0] 
    return content

def get_skype_content(SKYPE_FILE_NAME):
    skype_contents = open(SKYPE_FILE_NAME, 'r').readlines()
    skype_contents = ''.join(content).replace('\n', '\t\n')
    return skype_contents


if __name__ == "__main__":
    print("Auto 126 emailing...")
    
    #CONFIGURATIONS:
    #Email configuration:
    MIDDLE_FILE_NAME = "data/data_bonding_net/core_MV_SENDER_MAILBOXES_SKYPE_DRY_RUN.csv"
    EMAIL_FILE_NAME = 'data/data_bonding_net/email_content.txt'
    SKYPE_FILE_NAME = 'data/data_bonding_net/skype_content.txt'
    mail_host = "smtp.126.com"
    mail_sender = "limengxuan0708@126.com"
    mail_license = "lmx921221"  #this is not password!

    #Skype configuration:
    WAIT_TIME = 55
    username = 'mengxuan@bancosta.com'
    password = 'Bcchina2020'
    DRY_RUN = False
    DRY_RUN = True
 
    #Get data:
    middle_bond = get_middle_bond(MIDDLE_FILE_NAME)
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
            skypes_receivers = i.SKYPES.strip("[]'").replace(' ','').replace("','",",").replace("'","").replace("\"","").split(',')
            pic_skype_receiver = i.PIC_SKYPE.strip("[]'").replace(' ','').replace("','",",").replace("'","").replace("\"","").split(',')
        except Exception as e:
            print("Skip this row when getting PIC for %s."%i.MV)
            continue
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
    row_PIC = row_SKYPES
    #print(row_MV)
    #print(row_MAILBOXES)
    #print(row_PIC)

    #Emailing:
    for row_num,i in enumerate(middle_bond.iterrows()):
        this_MV = row_MV[row_num]
        mail_receivers = row_MAILBOXES[row_num]
        if len(mail_receivers) == 0:
            pass
        else:
            body_content = get_email_content(EMAIL_FILE_NAME, this_MV)
            print("*EMAIL* MV %s, Sending to %s"%(this_MV, mail_receivers))
            send_action(mail_sender, mail_receivers, subject_content, body_content)

    #Skyping:
    #Form chats content
    for row_num in row_MV.keys():
        row_MSG[row_num] = get_skype_content(row_MV[row_num])
    embed()
    sk = daily_bob.relentless_login_web_skype(username, password)
    struct_list = []
    for i,j,k in zip(all_target_people, [external_content]*len(all_target_people), [sk]*len(all_target_people)):
        struct_list.append([i,j,k])
    for row_num,i in enumerate(middle_bond.iterrows()):
        this_MV = row_MV[row_num]
        pic_skype_receiver = row_PIC[row_num]
        if len(pic_skype_receiver) == 0:
            pass
        else:
            print("*SKYPE* MV %s, Sending to %s"%(this_MV, pic_skype_receiver))
            #send_skype()
        
    print("All done!")
    #embed()
    
