from crypt import methods
from urllib import response
from flask import Flask, request
import movie_service as dynamodb

app = Flask(__name__)

@app.route('/titles/<director>/<int:year_start>/<int:year_end>',methods=['GET'])
def generate_titles_report(director,year_start,year_end):
    response=dynamodb.generate_titles_report(director,year_start,year_end)
    return response

@app.route('/titles/<int:user_review>',methods=['GET'])
def generate_english_titles(user_review):
    response=dynamodb.generate_english_titles(user_review)
    return response

@app.route('/titles/<country>/<int:year>',methods=['GET'])
def generate_highest_budget_titles(country,year):
    response=dynamodb.generate_highest_budget_titles(country,year)
    return response



if __name__ == '__main__':
    app.run(host='127.0.0.1', port=5000, debug=True)