# Download the helper library from https://www.twilio.com/docs/python/install
from twilio.rest import Client

# Your Account Sid and Auth Token from twilio.com/console
# DANGER! This is insecure. See http://twil.io/secure
account_sid = 'AC3f72ddfebffe2adae5b4efe0c6d9c9b6'
auth_token = 'd45d3fcfab5be3e08985b77a3fd13103'
client = Client(account_sid, auth_token)

from twilio.rest import TwilioRestClient
#填入你申请的号码
twilioNumber = '+19195253692'
#填入你验证的手机号
myNumbers = ["+8618601156335", "+8618601156335", "+8618601156335", "+8613120090157", "+8613120090157"]
#填入你想发送的信息

#Message
message = 'Hi this is ravana!'
client = Client(account_sid, auth_token)
for myNumber in myNumbers:
    print("Sending", myNumber)
    msg = client.messages.create(to=myNumber, from_=twilioNumber, body=message)
print(msg.sid)



#Phone call:
call = client.calls.create(
                        url='http://demo.twilio.com/docs/voice.xml',
                        to=myNumber,
                        from_=twilioNumber)
print(call.sid)



