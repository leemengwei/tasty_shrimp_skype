import smtplib
import email
from email.mime.text import MIMEText
from email.mime.image import MIMEImage
from email.mime.multipart import MIMEMultipart
from email.header import Header
from IPython import embed
import datetime
import pandas as pd

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

def get_body_content(CONTENT_FILE_NAME):
    content = open(CONTENT_FILE_NAME, 'r').readlines()
    content = ''.join(content).replace('\n', '\t\n')
    content = subject_content + '\n' + content + '\n' + str(datetime.datetime.now()).split(' ')[0] 
    return content
 
if __name__ == "__main__":
    print("Auto 126 emailing...")
    
    #CONFIGURATIONS:
    MIDDLE_FILE_NAME = "data/data_bonding_net/core_MV_SENDER_PIC_SKYPE_DRY_RUN.csv"
    CONTENT_FILE_NAME = 'data/data_bonding_net/email_content.txt'
    mail_host = "smtp.126.com"
    mail_sender = "limengxuan0708@126.com"
    mail_license = "lmx921221"  #this is not password!

    #Get data:
    middle_bond = get_middle_bond(MIDDLE_FILE_NAME)
    for i in middle_bond.iterrows():
        i = i[1]
        try:
            mail_receivers = i.PIC.strip("[]'").split("', '")
        except Exception as e:
            print("Skip this row when getting PIC for %s."%i.MV)
            continue
        try:
            mail_receivers.remove('')
        except:
            pass
        if len(mail_receivers) == 0:
            continue
        subject_content = 'MV %s/Suitable cargo - Bancsota desk'%i.MV
        body_content = get_body_content(CONTENT_FILE_NAME)
        print("MV %s, Sending to %s"%(i.MV, mail_receivers))
        send_action(mail_sender, mail_receivers, subject_content, body_content)
    
    print("All done!")
    #embed()
    
