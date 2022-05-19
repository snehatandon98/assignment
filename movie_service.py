from email import message
from tkinter.tix import COLUMN
import config
import boto3
import csv
import logging
from datetime import datetime
from time import time
from urllib import response
from boto3 import resource
from boto3.dynamodb.conditions import Key, Attr
from flask import jsonify

#Setting the AWS Credentials

AWS_ACCESS_KEY_ID = config.AWS_ACCESS_KEY_ID
AWS_SECRET_ACCESS_KEY = config.AWS_SECRET_ACCESS_KEY
REGION_NAME = config.REGION_NAME
ENDPOINT_URL = config.ENDPOINT_URL

#Importing DynamoDb as a resource

resource = resource(
    'dynamodb',
    endpoint_url = ENDPOINT_URL,
    aws_access_key_id     = AWS_ACCESS_KEY_ID,
    aws_secret_access_key = AWS_SECRET_ACCESS_KEY,
    region_name           = REGION_NAME
)

logging.basicConfig(level=logging.INFO)

#Reading the CSV File and passing the data to "write_to_dynamo" function
def populate_table():
    csvfile = open('Movie.csv')
    header = []
    header = next(csvfile)
    write_to_dynamo(csv.reader(csvfile,delimiter=",",quotechar='"'))
    t=datetime.now()
    logging.info("File Synced at " + str(t))

#Creating Movies Table
def create_table_movies():   
   table = resource.create_table(
       TableName = 'Movies', # Name of the table
        AttributeDefinitions = [
           {
               'AttributeName': 'imdb_id', # Name of the attribute
               'AttributeType': 'S'   # N = Number (B= Binary, S = String)
           },
           {
               'AttributeName': 'year', # Name of the attribute
               'AttributeType': 'N'   # N = Number (B= Binary, S = String)
           }
       ],
       KeySchema = [
           {
               'AttributeName': 'imdb_id',
               'KeyType'      : 'HASH' #RANGE = sort key, HASH = partition key
           },
           {
               'AttributeName': 'year',
               'KeyType'      : 'RANGE' #RANGE = sort key, HASH = partition key
           }
       ],
       ProvisionedThroughput={
           'ReadCapacityUnits'  : 10,
           'WriteCapacityUnits': 10
       }
   )
   logging.info("TABLE CREATED")
   return table
MovieTable = resource.Table('Movies')

#Inserting data from CSV File to Movies Table
def write_to_dynamo(rows):
    logging.info("Writing to DynamoDb started.")
    count = 0
    for col in rows:
        number=''
        count = count + 1
        if col[20]=='':
            col[20]=0
        if col[16]=='':
            col[16]=0
        else:
            for i in col[16]:
                if i.isdigit():
                    number+=i
            col[16]=int(number)
        response = MovieTable.put_item(
                Item={
                    'imdb_id' : col[0],
                    'title': col[1],
                    'original_title':col[2],
                    'year':int(col[3]),
                    'date_published':col[4],
                    'genre':col[5],
                    'duration':col[6],
                    'country':col[7],
                    'language':col[8],
                    'director':col[9],
                    'writer':col[10],
                    'production_company':col[11],
                    'actors':col[12],
                    'description':col[13],
                    'avg_vote':col[14],
                    'votes':col[15],
                    'budget':col[16],
                    'usa_gross_income':col[17],
                    'worlwide_gross_income':col[18],
                    'metascore':col[19],
                    'reviews_from_users':int(col[20]),
                    'reviews_from_critics':col[21],
                    
                }
            )
    logging.info("Writing to DynamoDb finished. Data succefully inserted into the Table.")
    

#Getting list of Movie Titles directed by given director 
# -in the given year range from the Database
def generate_titles_report(director,year_start,year_end):
        response=MovieTable.scan(
            FilterExpression=Attr('director').eq(director) & Attr('year').between(year_start,year_end)
            )
        titles=response['Items']
        if len(titles) ==0:
            return jsonify({'message': 'No records found for this request.'})
        else:
            result=[]
            for t in titles:
                result.append(t['title'])
            return jsonify(result)

#Getting list of English Movie titles which have user reviews more 
#than given user review from the database in descending order.
def generate_lang_titles(language,user_review):
    response=MovieTable.scan(
        FilterExpression=Attr('language').contains(language)  & Attr('reviews_from_users').gte(user_review)
    )
    titles=response['Items']
    res=[]
    result=[]
    if len(titles)==0:
        return jsonify({'message': 'No records found for this request.'})
    else:
        for t in titles:
            res.append((t['reviews_from_users'],t['title']))
        res.sort(key = lambda x: x[0],reverse=True)
        result = [x[1] for x in res]
        return jsonify(result)


#Getting the highest budget title for the given year and country from the database.
def generate_highest_budget_titles(country,year):
    response=MovieTable.scan(
        FilterExpression=Attr('country').contains(country) & Attr('year').eq(year)
    )   
    titles=response['Items']
    b=0
    if len(titles)==0:
        return jsonify({'message': 'No records found for this request.'})
    else:
        title=''
        for t in titles:
            if t['budget']>b:
                title=t['title']
                b=t['budget']
        if b==0:
            return jsonify({'message': 'No records found with budget for this request.'})
        return jsonify(title)
