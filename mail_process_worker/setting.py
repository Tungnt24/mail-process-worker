from environs import Env

env = Env()
env.read_env()


class KafkaConsumerConfig:
    KAFKA_BROKER = env.list("KAFKA_BROKER")
    KAFKA_CONSUMER_TOPIC = env.list("KAFKA_CONSUMER_TOPIC")
    KAFKA_CONSUMER_GROUP = env.str("KAFKA_CONSUMER_GROUP")
    KAFKA_ENABLE_AUTO_COMMIT = env.bool("KAFKA_ENABLE_AUTO_COMMIT")
    KAFKA_AUTO_OFFSET_RESET = env.str("KAFKA_AUTO_OFFSET_RESET")
    KAFKA_MAX_POLL_RECORDS = env.int("KAFKA_MAX_POLL_RECORDS")
    KAFKA_POLL_TIMEOUT = env.int("KAFKA_POLL_TIMEOUT")


class KafkaAuth:
    SASL_PLAIN_USERNAME = env.str("SASL_PLAIN_USERNAME")
    SASL_PLAIN_PASSWORD = env.str("SASL_PLAIN_PASSWORD")
    SECURITY_PROTOCOL = env.str("SECURITY_PROTOCOL")
    SASL_MECHANISM = env.str("SASL_MECHANISM")


class MQTTConfig:
    CLIENT_ID = env.str("CLIENT_ID")
    MQTT_BROKER = env.str("MQTT_BROKER")
    MQTT_PORT = env.int("MQTT_PORT")
    MQTT_USERNAME = env.str("MQTT_USERNAME")
    MQTT_PASSWORD = env.str("MQTT_PASSWORD")
    MQTT_TOPIC = env.str("MQTT_TOPIC")
    MQTT_QoS = env.int("MQTT_Qos")


class WorkerConfig:
    WINDOW_DURATION = env.int("WINDOW_DURATION")
    NUMBER_OF_MESSAGE = env.int("NUMBER_OF_MESSAGE")
