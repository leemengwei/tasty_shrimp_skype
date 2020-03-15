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
    mm["To"] = ', '.join(["<%s>"%i for i in mail_receivers])
    mm["Subject"] = Header(subject_content, 'utf-8')
    #content:
    message_text = MIMEText(body_content, "plain", "utf-8")
    mm.attach(message_text)
    
    #attach a jpg
    image_data = open('data/portrait.jpg', 'rb')
    message_image = MIMEImage(image_data.read())
    image_data.close()
    #mm.attach(message_image)

    #log in service:
    stp = smtplib.SMTP()
    stp.connect(mail_host, 25)  
    stp.set_debuglevel(0)
    stp.login(mail_sender,mail_license)

    #send:
    stp.sendmail(mail_sender, mail_receivers, mm.as_string())
    print("邮件发送成功")
    stp.quit()
    return

def get_middle_data(MIDDLE_FILE_NAME):
    data = pd.read_csv(DATA_PATH_PREFIX+MIDDLE_FILE_NAME)
    return data

if __name__ == "__main__":
    print("Auto 126 emailing...")
    
    #CONFIGURATIONS:
    DATA_PATH_PREFIX = './output/'
    MIDDLE_FILE_NAME = "core_MV_SENDER_PIC_DRY_RUN.csv"
    mail_host = "smtp.126.com"
    mail_sender = "limengxuan0708@126.com"
    mail_license = "lmx921221"  #this is not password!

    #Get data:
    middle_data = get_middle_data(MIDDLE_FILE_NAME)
    for i in middle_data.iterrows():
        i = i[1]
        mail_receivers = i.PIC.strip("[]'").split("', '")
        subject_content = 'For M/V: %s'%i.MV
        body_content = "测试%s"%datetime.datetime.now()
        print("MV %s, Sending to %s"%(i.MV, mail_receivers))
        send_action(mail_sender, mail_receivers, subject_content, body_content)
    
    print("All done!")
    #embed()
    
