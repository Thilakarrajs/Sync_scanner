import smtplib, ssl
from email import encoders
from email.mime.base import MIMEBase
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from loguru import logger

class MailHelper:
    @staticmethod
    def sendMailNotification(fileName, readerObj):
        logger.info("sendMailNotification method invoked ")
        smtp_server = readerObj.getConfigValue('SMTP_CONFIG', 'HOST')
        port = readerObj.getConfigValue('SMTP_CONFIG', 'PORT')
        sender_email = readerObj.getConfigValue('SMTP_CONFIG', 'USER_NAME')
        password = readerObj.getConfigValue('SMTP_CONFIG', 'PASSWORD')
        context = ssl.create_default_context()
        receiver_email =readerObj.getConfigValue('SMTP_CONFIG', 'RECEVIER_MAIL')
        subject ='Sync Scanner Reports'
        try:
            message = MIMEMultipart()
            message["From"] = sender_email
            message["To"] = receiver_email
            message["Subject"] = subject
            server = smtplib.SMTP(smtp_server,port)
            #server.ehlo() # Can be omitted
            server.starttls(context=context) # Secure the connection
            server.ehlo() # Can be omitted
            server.login(sender_email, password)
            html = """\
                <html>
                  <body>
                    <p>Hi,<br>
                        Following are the sync scanner report 
                    </p>
                  </body>
                </html>
                """
            part2 = MIMEText(html, "html")
            with open(fileName, "rb") as attachment:
                part = MIMEBase("application", "octet-stream")
                part.set_payload(attachment.read())
            encoders.encode_base64(part)
            part.add_header(
                "Content-Disposition",
                f"attachment; filename= scan_report.csv",
            )
            message.attach(part2)
            message.attach(part)
            server.sendmail(
                sender_email, receiver_email, message.as_string()
            )
            logger.info('Mail send successfully')
        except Exception as e:
                logger.error(e)
        finally:
            server.quit()