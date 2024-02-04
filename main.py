import json
import boto3
import datetime

sqs = boto3.resource("sqs")


def serialize_datetime(obj):
    if isinstance(obj, datetime.datetime):
        return obj.isoformat()
    raise TypeError("Type not serializable")


if __name__ == "__main__":
    # data = open('resources/jsons/person-individual.json')
    # data = open('resources/jsons/person-legal.json')
    # data = open('resources/jsons/receive-account.json')
    data = open('resources/jsons/test.json')
    data = json.load(data)

    queue = sqs.get_queue_by_name(QueueName="lambda_data_account_normalizer")

    # Late
    entries = [
        {
            "Id": str(ind),
            "MessageBody": json.dumps(data.get('payload')),
            "MessageAttributes": data.get('attributes'),
        }
        for ind in range(10)

    ]

    # Individual
    # queue.send_messages(Entries=entries)
    # Late
    queue.send_message(MessageBody=json.dumps(data.get('payload'), default=serialize_datetime),
                       MessageAttributes=data.get('attributes'))
