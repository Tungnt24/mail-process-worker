from mail_process_worker.logic.handle_kafka_event import HandleEvent


def main():
    event = HandleEvent()
    event.aggregate_event_by_amount()


if __name__ == "__main__":
    main()
