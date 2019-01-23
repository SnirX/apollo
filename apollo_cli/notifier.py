from abc import ABCMeta, abstractmethod
from slackclient import SlackClient


class NotificationSender:
    __metaclass__ = ABCMeta

    @abstractmethod
    def send_notification(self, *args, **kwargs):
        pass


class SlackNotificationSender(NotificationSender):

    def __init__(self, token, channel):
        self.channel = channel
        self.token = token
        self.client = SlackClient(self.token)

    def send_notification(self, title, message, status="success"):
        self.client.api_call(
            "chat.postMessage",
            channel=self.channel,
            attachments=self.generate_message(title, message, status)
        )

    @staticmethod
    def generate_message(title, message, status):
        if status == "success":
            color = "good"
        else:
            color = "danger"

        message = [{
            "title": title,
            "title_link": "https://api.slack.com/",
            "text": message,
            "color": color,
            "fields": [
                {
                    "title": title,
                    "short": False
                }
            ]
        }]

        return message

