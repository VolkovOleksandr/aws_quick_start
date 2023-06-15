import traceback
import logging
from boto3.dynamodb.conditions import Key
from botocore.exceptions import ClientError

from utils.db_helper import connect_dynamodb

logger = logging.getLogger(__name__)

class Users():
    
    def __init__(self):
        # Connect AWS DynamoDb and table
        self.dyn_resource = connect_dynamodb()
        self.table = self.dyn_resource.Table('users')

    def exists(self):
        """
        Determines whether a table exists. As a side effect, stores the table in
        a member variable.

        :return: True when the table exists; otherwise, False.
        """

        try:
            table = self.dyn_resource.Table('users')
            table.load()
            exists = True

        except ClientError as err:
            if err.response['Error']['Code'] == 'ResourceNotFoundException':
                exists = False
            else:
                logger.error(
                    "Couldn't check for existence of %s. Here's why: %s: %s",
                    'users',
                    err.response['Error']['Code'], err.response['Error']['Message'])
                raise
        else:
            self.table = table

        return exists
    
    def list_tables(self):
        """
        Lists the Amazon DynamoDB tables for the current account.

        :return: The list of tables.
        """
        try:
            tables = []
            for table in self.dyn_resource.tables.all():
                print(table.name)
                tables.append(table)

        except ClientError as err:
            logger.error(
                "Couldn't list tables. Here's why: %s: %s",
                err.response['Error']['Code'], err.response['Error']['Message'])
            raise

        else:
            return tables
        
    def create_table(self, table_name):
        """
        Creates an Amazon DynamoDB table.
        The table uses the release user_id of the user as the partition key and the
        full_name as the sort key.

        :param table_name: The name of the table to create.
        :return: The newly created table.
        """

        try:
            self.table = self.dyn_resource.create_table(
                TableName=table_name,
                KeySchema=[
                    {'AttributeName': 'user_id', 'KeyType': 'HASH'},  # Partition key
                    {'AttributeName': 'full_name', 'KeyType': 'RANGE'}  # Sort key
                ],
                AttributeDefinitions=[
                    {'AttributeName': 'user_id', 'AttributeType': 'S'},
                    {'AttributeName': 'full_name', 'AttributeType': 'S'}
                ],
                BillingMode='PAY_PER_REQUEST',
            )
            
            self.table.wait_until_exists()

        except ClientError as err:
            logger.error(
                "Couldn't create table %s. Here's why: %s: %s", table_name,
                err.response['Error']['Code'], err.response['Error']['Message'])
            raise

        else:
            return self.table
    
    def delete_table(self):
        """
        Deletes the table.
        """
        try:
            self.table.delete()
            self.table = None

        except ClientError as err:
            logger.error(
                "Couldn't delete table. Here's why: %s: %s",
                err.response['Error']['Code'], err.response['Error']['Message'])
            raise

    def add_user(self, user_id, full_name):
        """
        Adds a user to the table.

        :param user_id: The uuid.
        :param full_name: The user name.
        """

        try:
            response = self.table.put_item(
                Item={
                    'user_id': user_id,
                    'full_name': full_name
                }
            )
            return response
        
        except Exception:
            print("ERROR: ", traceback.format_exc())
            return None

    def query_by_user_id(self, user_id):
        """
        Queries for user by user_id.

        :param user_id: The user's id.
        :return: The user record.
        """

        try:
            response = self.table.query(
                KeyConditionExpression=Key('user_id').eq(user_id)
            )
            return response['Items']
        
        except Exception:
            print("ERROR: ", traceback.format_exc())
            return None
        
    def query_by_user_name(self, name):
        """
        Queries for users by user name.

        :param name: The name to query.
        :return: The users record.
        """
        
        try:
            response = self.table.query(
                IndexName='name-index',
                KeyConditionExpression=Key('name').eq(name)
            )
            return response['Items']

        except Exception:
            print("ERROR: ", traceback.format_exc())
            return None
        
    def update_user(self, user_id, full_name, **kwargs):
        """
        Update user in the table.

        :param user_id: The user of the record to update.
        :param full_name: The search key of the record to update.
        :param **kwargs: The key params to update.
        :return: The fields that were updated, with their new values.
        """
        
        try:
            update_expression = []
            expression_attributes = {}

            for k, v in kwargs.items():
                update_expression.append(f"{k}=:{k}")
                expression_attributes[f":{k}"] = v

            update_expression = "set " + ", ".join(update_expression)
            
            response = self.table.update_item(
                Key={
                    'user_id': user_id,
                    'full_name': full_name
                },
                UpdateExpression = update_expression,
                ExpressionAttributeValues = expression_attributes,
                ReturnValues="UPDATED_NEW")
            
            return response['Attributes']
        
        except Exception:
            print(traceback.format_exc())
            return None
    
    def delete_user_by_id(self, user_id, full_name):
        """
        Deletes a user from the table.

        :param user_id: The id of the user to delete.
        :param full_name: The additionl search key.
        """

        try:
            self.table.delete_item(Key={'user_id': user_id, 'full_name': full_name})
            return True
        
        except ClientError as err:
            print("Error: ", traceback.format_exc())
            return False
            