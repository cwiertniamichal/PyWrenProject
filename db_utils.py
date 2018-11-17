import json
import decimal
from typing import Set

import boto3
from boto3.dynamodb.conditions import Key
from botocore.exceptions import ClientError


dynamo_db = boto3.resource('dynamodb', region_name='us-east-1')


NODE_ID = 'node_id'
ARTICLE_TITLE = 'article_title'
LINKS = 'links'

ARTICLE_TITLE_KEY = {
    'AttributeName': ARTICLE_TITLE,
    'KeyType': 'HASH'
}

ARTICLE_TITLE_ATTRIBUTE = {
    'AttributeName': ARTICLE_TITLE,
    'AttributeType': 'S'
}

LINKS_ATTRIBUTE = {
    'AttributeName': LINKS,
    'AttributeType': 'L'
}

PROVISIONED_THROUGHPUT = {
    'ReadCapacityUnits': 1000,
    'WriteCapacityUnits': 1000
}


def initialize_db():
    table_names = [table.name for table in dynamo_db.tables.all()]

    if 'Nodes' not in table_names:
        dynamo_db.create_table(
            TableName='Nodes',
            KeySchema=[ARTICLE_TITLE_KEY],
            AttributeDefinitions=[ARTICLE_TITLE_ATTRIBUTE],
            ProvisionedThroughput=PROVISIONED_THROUGHPUT
        )

    if 'Edges' not in table_names:
        dynamo_db.create_table(
            TableName='Edges',
            KeySchema=[ARTICLE_TITLE_KEY],
            AttributeDefinitions=[ARTICLE_TITLE_ATTRIBUTE],
            ProvisionedThroughput=PROVISIONED_THROUGHPUT
        )


def get_nodes_table():
    return get_table('Nodes')


def get_edges_table():
    return get_table('Edges')


def get_table(table_name: str):
    return dynamo_db.Table(table_name)


def get_links(article_title: str):
    table = get_nodes_table()
    key_condition_expression = Key(ARTICLE_TITLE).eq(article_title)

    resp = table.query(
        KeyConditionExpression=key_condition_expression
    )
    return next(item[LINKS] for item in resp['Items'])


def is_article_in_nodes_table(article_title: str):
    table = get_nodes_table()
    key_condition_expression = Key(ARTICLE_TITLE).eq(article_title)

    resp = table.query(
        KeyConditionExpression=key_condition_expression
    )

    if resp['Items']:
        return True
    return False


def delete_table(table_name: str):
    table = get_table(table_name)
    table.delete()


def add_node(article_title: str, links: Set):
    table = get_nodes_table()
    item = {
            ARTICLE_TITLE: article_title,
            LINKS: links
    }

    table.put_item(
        Item=item
    )


def add_edge(article_title: str, link: str):
    table = get_edges_table()
    key = {
        ARTICLE_TITLE: article_title
    }
    update_expression = ('ADD {links} :link'.format(links=LINKS))
    expression_attribute_values = {
        ':link': {link}
    }
    condition_expression = 'attribute_exists({})'.format(LINKS)

    try:
        r = table.update_item(
            Key=key,
            UpdateExpression=update_expression,
            ConditionExpression=condition_expression,
            ExpressionAttributeValues=expression_attribute_values,
            ReturnValues='ALL_NEW'
        )

    except ClientError as e:
        item = {
            ARTICLE_TITLE: article_title,
            LINKS: {link}
        }
        r = table.put_item(
            Item=item
        )


def scan_table(table_name: str):
    table = get_table(table_name)
    resp = table.scan()

    for item in resp['Items']:
        print(item)


# Helper class to convert a DynamoDB item to JSON.
class DecimalEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, decimal.Decimal):
            if o % 1 > 0:
                return float(o)
            else:
                return int(o)
