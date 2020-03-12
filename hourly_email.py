import smtplib
import email
from email.mime.text import MIMEText
from email.mime.image import MIMEImage
from email.mime.multipart import MIMEMultipart
from email.header import Header
from IPython import embed
import datetime

def parse_header(mm):
    subject_content = """也是电脑发"""
    mm["From"] = "<limengxuan0708@126.com>"
    mm["To"] = "<limengxuan0708@163.com>, <mengxuan@bancosta.com>, <limengxuan0708@126.com>"
    mm["Subject"] = Header(subject_content, 'utf-8')
    return mm

def parse_content(mm):
    #content:
    body_content = "测试%s"%datetime.datetime.now()
    message_text = MIMEText(body_content, "plain", "utf-8")
    mm.attach(message_text)
    
    #attach a jpg
    image_data = open('data/portrait.jpg', 'rb')
    message_image = MIMEImage(image_data.read())
    image_data.close()
    #mm.attach(message_image)
    return mm
    
def send_action(mail_receivers):
    #MAIN:
    mm = MIMEMultipart('related')
    mm = parse_header(mm)
    mm = parse_content(mm)

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
    
if __name__ == "__main__":
    print("Auto 126 emailing...")
    
    #CONFIGURATIONS:
    mail_host = "smtp.126.com"
    mail_sender = "limengxuan0708@126.com"
    mail_license = "lmx921221"  #not password!
    mail_receivers = ["limengxuan0708@163.com", "limengxuan0708@126.com", "mengxuan@bancosta.com"]
    
    #SEND:
    print("Will send:", mail_receivers)
    send_action(mail_receivers)
    
    print("Sent!")
    #embed()
    
