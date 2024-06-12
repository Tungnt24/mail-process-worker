import time
import calendar

from mail_process_worker.utils.logger import logger
from mail_process_worker.utils.decorator import timeout
from mail_process_worker.logic.client.kafka_client import KafkaConsumerClient, KafkaProducerClient
from mail_process_worker.logic.client.redis_client import rdb
from mail_process_worker.setting import WorkerConfig


class HandleEvent:
    def __init__(self) -> None:
        self.user_events = {}
        self.new_event = {}
        self.messages = []
        self.producer = KafkaProducerClient()
        self.consumer = KafkaConsumerClient()
        self.consumer.create_consumer()

    def get_current_timestamp(self):
        timestamp = calendar.timegm(time.gmtime())
        return timestamp

    def set_priority(self, data: dict):
        if len(self.messages) == WorkerConfig.NUMBER_OF_MESSAGE:
            self.producer.ordered_message(self.user_events)
            self.producer.send_message(self.consumer.consumer)
            self.user_events.clear()
            self.new_event.clear()
            self.messages.clear()

        self.messages.append(data)
        event_priority = {
            "MailboxCreate": 1,
            "MailboxRename": 2,
            "MessageNew": 3,
            "MessageAppend": 4,
            "FlagsSet": 5,
            "FlagsClear": 5,
            "MessageExpunge": 6,
            "MessageTrash": 7,
            "MailboxDelete": 8,
        }
        event_name = data["event"]
        user = data.get("user", None)
        if not user:
            return
        exist_user = self.user_events.get(user, None)
        if not exist_user:
            self.user_events.update({user: []})
        self.user_events[user].append((event_priority[event_name], data))

    @timeout(10)
    def custom_event(self, data: dict):
        user = data["user"]
        if data["event"] == "MessageAppend":
            exist_user = self.new_event.get(user, None)
            if not exist_user:
                self.new_event.update(
                    {
                        user: {
                            "new_uids": [],
                        }
                    }
                )
            self.new_event[user]["new_uids"].append(data["uid"])
            self.new_event[user].update(
                {
                    "event": "MessageMove",
                    "event_timestamp": self.get_current_timestamp(),
                    "user": user,
                    "new_mailbox": data["mailbox"],
                }
            )
        elif data["event"] == "MessageExpunge":
            self.new_event[user].update(
                {
                    "old_uids": [],
                    "old_mailbox": data["mailbox"],
                    "offset": data["offset"],
                    "topic": data["topic"],
                    "partition": data["partition"],
                }
            )
            self.new_event[user]["old_uids"].append(data["uid"])
            self.set_priority(self.new_event[user])
        return None

    def handle_event(self, event):
        data = event.value
        if data["event"] in [
            "MessageRead",
            "MailboxSubscribe",
            "MailboxUnsubscribe",
        ]:
            return

        data.update(
            {
                "topic": event.topic,
                "partition": event.partition,
                "offset": event.offset,
            }
        )

        logger.info(f"Received: {data}")
        if data["event"] == "MessageAppend" and data["user"] in data.get(
            "from", ""
        ):
            return self.set_priority(data)
        return self.set_priority(data)

    def delay_event(self, user, message_id_header):
        if not message_id_header:
            return
        message_id_header = message_id_header.strip()
        key = '{key}_{email}_{msg_id_header}'.format(
            email=user,
            key='DISTRIBUTED_LOCK',
            msg_id_header=message_id_header
        )
        for _ in range(150):
            if rdb.hget("lock", key):
                logger.info("DISTRIBUTED_LOCK!!!! KEY: {}".format(key))
                time.sleep(0.1)
            else:
                break
            
    def aggregate_event_by_amount(self):
        start = time.time()
        while True:
            if time.time() - start > WorkerConfig.WINDOW_DURATION:
                self.producer.ordered_message(self.user_events)
                self.producer.send_message(self.consumer.consumer)
                self.user_events.clear()
                self.new_event.clear()
                self.messages.clear()
                start = time.time()
            else:
                msg = self.consumer.poll_message()
                if not msg:
                    continue
                start = time.time()
                logger.info("MESSAGE: {}".format(msg))
                for _, events in msg.items():
                    for event in events:
                        self.handle_event(event)
