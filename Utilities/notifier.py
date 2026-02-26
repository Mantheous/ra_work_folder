import smtplib
from email.message import EmailMessage

def notify(email="mantheous@gmail.com", message="Scrapper has crashed", subject = "Scrapper Error"):
    """
    Just call this fuction to get an email.

    """
    msg = EmailMessage()
    msg.set_content(message)
    msg['Subject'] = subject
    msg['From'] = "mantheousnotifier@gmail.com"
    msg['To'] = email

    # Connect to Gmail's SMTP server
    with smtplib.SMTP_SSL('smtp.gmail.com', 465) as server:
        server.login("mantheousnotifier@gmail.com", "lzol vsce wnkt qhnh")
        server.send_message(msg)

if __name__ == "__main__":
    notify()