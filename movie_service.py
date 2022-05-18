from urllib import response
from boto3 import resource
import boto3
from boto3.dynamodb.conditions import Key, Attr
import csv

from flask import jsonify
import config

AWS_ACCESS_KEY_ID = config.AWS_ACCESS_KEY_ID
AWS_SECRET_ACCESS_KEY = config.AWS_SECRET_ACCESS_KEY
REGION_NAME = config.REGION_NAME
ENDPOINT_URL = config.ENDPOINT_URL

resource = resource(
    'dynamodb',
    endpoint_url = ENDPOINT_URL,
    aws_access_key_id     = AWS_ACCESS_KEY_ID,
    aws_secret_access_key = AWS_SECRET_ACCESS_KEY,
    region_name           = REGION_NAME
)
def main():

    #create_table_movies()
    csvfile = open('Movie.csv')
    header = []
    header = next(csvfile)
    write_to_dynamo(csv.reader(csvfile,delimiter=",",quotechar='"'))
    print("Done")
    generate_titles_report("D.W. Griffith",1900,2010)
    print("-----------------------------------------------------")
    generate_english_titles(9)
    print("-------------------------------------------------------------")
    generate_highest_budget_titles('France',1921)

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
   return table
MovieTable = resource.Table('Movies')

def write_to_dynamo(rows):
    count = 0
    for row in rows:
        number=''
        count = count + 1
        if row[20]=='':
            row[20]=0
        if row[16]=='':
            row[16]=0
        else:
            for i in row[16]:
                if i.isdigit():
                    number+=i
            row[16]=int(number)
        response = MovieTable.put_item(
                Item={
                    'imdb_id' : row[0],
                    'title': row[1],
                    'original_title':row[2],
                    'year':int(row[3]),
                    'date_published':row[4],
                    'genre':row[5],
                    'duration':row[6],
                    'country':row[7],
                    'language':row[8],
                    'director':row[9],
                    'writer':row[10],
                    'production_company':row[11],
                    'actors':row[12],
                    'description':row[13],
                    'avg_vote':row[14],
                    'votes':row[15],
                    'budget':row[16],
                    'usa_gross_income':row[17],
                    'worlwide_gross_income':row[18],
                    'metascore':row[19],
                    'reviews_from_users':int(row[20]),
                    'reviews_from_critics':row[21],
                    
                }
            )

def generate_titles_report(director,year_start,year_end):
        response=MovieTable.scan(
            FilterExpression=Attr('director').eq(director) & Attr('year').between(year_start,year_end)
            )
        titles=response['Items']
        result=[]
        for t in titles:
            result.append(t['title'])
        return jsonify(result)

def generate_english_titles(user_review):
    response=MovieTable.scan(
        FilterExpression=Attr('language').eq('English')  & Attr('reviews_from_users').gte(user_review)
    )
    titles=response['Items']
    result=[]
    for t in titles:
        result.append(t['title'])
    return jsonify(result)
def generate_highest_budget_titles(country,year):
    response=MovieTable.scan(
        FilterExpression=Attr('country').contains(country) & Attr('year').eq(year)
    )   
    titles=response['Items']
    b=0
    title=''
    #print(titles)
    for t in titles:
        if t['budget']>b:
            title=t['title']
            b=t['budget']
    return jsonify(title)
if __name__ == '__main__':
    main()