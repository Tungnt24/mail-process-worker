import json
import ssl

from kafka import KafkaConsumer, KafkaProducer
from kafka.structs import TopicPartition, OffsetAndMetadata

from mail_process_worker.setting import KafkaClientConfig, KafkaAuth
from mail_process_worker.utils.logger import logger
from mail_process_worker.utils.decorator import retry, timeout

AGGREGATE = ["MessageAppend", "MessageExpunge", "FlagsSet", "FlagsClear", "MessageTrash"]

context = ssl.create_default_context()
context.options &= ssl.OP_NO_TLSv1
context.options &= ssl.OP_NO_TLSv1_1

class KafkaConsumerClient:
    def __init__(self) -> None:
        self.consumer = None
        self.topics = KafkaClientConfig.KAFKA_CONSUMER_TOPIC
        self.group_id = KafkaClientConfig.KAFKA_CONSUMER_GROUP
        self.bootstrap_servers = KafkaClientConfig.KAFKA_BROKER
        self.auto_offset_reset = KafkaClientConfig.KAFKA_AUTO_OFFSET_RESET
        self.value_deserializer = lambda x: json.loads(
            x.decode("utf-8", "ignore")
        )
        self.enable_auto_commit = KafkaClientConfig.KAFKA_ENABLE_AUTO_COMMIT
        self.max_poll_records = KafkaClientConfig.KAFKA_MAX_POLL_RECORDS
        self.poll_timeout = KafkaClientConfig.KAFKA_POLL_TIMEOUT
        self.sasl_plain_username = KafkaAuth.SASL_PLAIN_USERNAME
        self.sasl_plain_password = KafkaAuth.SASL_PLAIN_PASSWORD
        self.security_protocol = KafkaAuth.SECURITY_PROTOCOL
        self.sasl_mechanism = KafkaAuth.SASL_MECHANISM
        self.ssl_context = context

    @retry(times=3, delay=1)
    @timeout(10)
    def create_consumer(self):
        logger.info(self.bootstrap_servers)
        self.consumer = KafkaConsumer(
            *self.topics,
            group_id=self.group_id,
            bootstrap_servers=self.bootstrap_servers,
            auto_offset_reset=self.auto_offset_reset,
            value_deserializer=self.value_deserializer,
            enable_auto_commit=self.enable_auto_commit,
            max_poll_records=self.max_poll_records,
            sasl_plain_username=self.sasl_plain_username,
            sasl_plain_password=self.sasl_plain_password,
            security_protocol=self.security_protocol,
            ssl_context=self.ssl_context,
            sasl_mechanism=self.sasl_mechanism
        )

    def poll_message(self):
        msg = self.consumer.poll(self.poll_timeout)
        return msg

    @staticmethod
    def kafka_commit(consumer, topic, partition, offset):
        tp = TopicPartition(topic, partition)
        consumer.commit({tp: OffsetAndMetadata(offset + 1, None)})
        logger.info(
            f"KAFKA COMMIT - TOPIC: {topic} - PARTITION: {partition} - OFFSET: {offset}"
        )


class KafkaProducerClient:
    def __init__(self) -> None:
        self.bootstrap_servers = KafkaClientConfig.KAFKA_BROKER
        self.normal_topic = KafkaClientConfig.KAFKA_PRODUCER_NORMAL_TOPIC
        self.aggregated_topic = (
            KafkaClientConfig.KAFKA_PRODUCER_AGGREGATED_TOPIC
        )
        self.topic_for_transfer = KafkaClientConfig.KAFKA_PRODUCER_TRANSFER_TOPIC
        self.value_serializer = lambda x: json.dumps(x).encode("utf-8")
        self.kafka_msgs = []
        self.sasl_plain_username = KafkaAuth.SASL_PLAIN_USERNAME
        self.sasl_plain_password = KafkaAuth.SASL_PLAIN_PASSWORD
        self.security_protocol = KafkaAuth.SECURITY_PROTOCOL
        self.sasl_mechanism = KafkaAuth.SASL_MECHANISM
        self.ssl_context = context

    def ordered_message(self, user_messages: dict):
        for user in user_messages:
            messages = user_messages[user]
            messages.sort(key=lambda x: x[0])
            for priority, message in messages:
                self.create_kafka_message(message)

    def create_kafka_message(self, message: dict):
        uids = len(message.get("uids", []))
        user = message.get("user")
        username, _, domain = user.partition("@")
        msg_format = {"payload": message}
        if user in KafkaClientConfig.KAFKA_EMAIL_TRANSFER:
            topic = self.topic_for_transfer
            msg_format.update({"key": user, "topic": topic})
        elif uids > 1 or message.get("event") in AGGREGATE:
            if domain in KafkaClientConfig.KAFKA_IGNORE_DOMAIN:
                return
            topic = self.aggregated_topic
            msg_format.update({"key": user, "topic": topic})
        else:
            topic = self.normal_topic
            msg_format.update({"key": user, "topic": topic})
        self.kafka_msgs.append(msg_format)

    @retry(times=3, delay=1, logger=logger)
    @timeout(60)
    def send_message(self, consumer: KafkaConsumer):
        producer = KafkaProducer(
            bootstrap_servers=self.bootstrap_servers,
            value_serializer=self.value_serializer,
            sasl_plain_username=self.sasl_plain_username,
            sasl_plain_password=self.sasl_plain_password,
            security_protocol=self.security_protocol,
            ssl_context=self.ssl_context,
            sasl_mechanism=self.sasl_mechanism,
            acks="all"
        )
        for msg in self.kafka_msgs:
            payload = msg.get("payload", {})
            kafka_topic = msg.get("topic")
            kafka_key = msg.get("key")
            logger.info("Sending message: {} to topic: {}".format(payload, kafka_topic))
            uids = payload.get("uids") or []
            slice = KafkaClientConfig.KAFKA_SLICE_SIZE
            while len(uids) >= slice:
                p = uids[:slice]
                uids = uids[slice:]
                payload["uids"] = p
                producer.send(kafka_topic, key=bytes(kafka_key, "utf-8"), value=payload)
                producer.flush()
            else:
                if uids:
                    payload["uids"] = uids                
                    producer.send(kafka_topic, key=bytes(kafka_key, "utf-8"), value=payload)
                producer.flush()
            self.commit(consumer, payload)
        self.kafka_msgs.clear()

    def commit(self, consumer, payload):
        event_topic = payload.get("topic")
        partition = payload.get("partition")
        offset = payload.get("offset")
        tp = TopicPartition(event_topic, partition)
        consumer.commit({tp: OffsetAndMetadata(offset + 1, None)})
        logger.info(
            f"KAFKA COMMIT - TOPIC: {event_topic} - PARTITION: {partition} - OFFSET: {offset}"
        )
