import smtplib, ssl
import Data.config_read as configuration


class send_email:
    def __init__(self):
        config = configuration.config()
        self.username = config.email_username
        self.password = config.email_password

    def daily_update_email(self):
        port = 465  # For SSL

        print(self.username)
        print(self.password)

        context = ssl.create_default_context()
        sender_email = self.username
        receiver_email = "pjk1413@gmail.com"
        message = """\
        Subject: Hi there

        This message is sent from Python."""

        with smtplib.SMTP_SSL("smtp.gmail.com", port, context=context) as server:
            server.login(self.username, self.password)
            server.sendmail(sender_email, receiver_email, message)
