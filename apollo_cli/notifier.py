from abc import ABCMeta, abstractmethod
from slackclient import SlackClient
from apollo_exceptions import NotificationError


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

    def send_notification(self, title, node, status="success"):
        self.client.api_call(
            "chat.postMessage",
            channel=self.channel,
            attachments=self.generate_message(title, node, status)
        )

    @staticmethod
    def generate_message(title, node, status):
        if status == "success":
            color = "good"
        elif status == "failure":
            color = "danger"
        elif status == "normal":
            color = "#3399FF"
        else:
            raise NotificationError("incorrect alert color")

        message = [{
            "title": title,
            "title_link": "https://api.slack.com/",
            "color": color,
            "fields": [
                {
                    "title": node,
                    "short": False
                }
            ]
        }]

        return message

