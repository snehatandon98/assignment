import movie_service as dynamodb
import jwt
import datetime
import config
import time
import logging
from jwt import decode
from functools import wraps
from apscheduler.schedulers.background import BackgroundScheduler
from crypt import methods
from datetime import datetime
from urllib import response
from flask import Flask, request,jsonify, Response, make_response


app = Flask(__name__)
app.config['SECRET_KEY']=config.SECRET_KEY
scheduler = BackgroundScheduler()
def token_required(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        print(app.config['SECRET_KEY'])
        token = None
        if 'x-access-token' in request.headers:
            token = request.headers['x-access-token']

        if not token:
            return jsonify({'message' : 'Token is missing!'}), 401

        try: 
            data = jwt.decode(token, app.config['SECRET_KEY'],algorithms=["HS256"])
        except:
            return jsonify({'message' : 'Token is invalid!'}), 401

        return f(*args, **kwargs)

    return decorated

@app.route('/createTable')
def root_route():
    dynamodb.create_table_movies()
    return 'Table created'

def sync():
    dynamodb.populate_table()


#Getting list of Movie Titles directed by given director in the given year range
@app.route('/titles/<director>/<int:year_start>/<int:year_end>',methods=['GET'])
@token_required
def generate_titles_report(director,year_start,year_end):
    start=time.time()
    try:
        response=dynamodb.generate_titles_report(director,year_start,year_end)
        
    except:
        response= jsonify({'message' : 'Invalid request'})
    end=time.time()
    total=int(round(1000*(end-start)))
    response.headers["X-TIME-TO-EXECUTE"]=total
    return response


#Getting list of English Movie titles which have user reviews more than given user review 
@app.route('/titles/<int:user_review>',methods=['GET'])
@token_required
def generate_english_titles(user_review):
    start = time.time()
    try:
        response=dynamodb.generate_english_titles(user_review)
    except:
        response= jsonify({'message' : 'Invalid request'})
    end=time.time()
    total=int(round(1000*(end-start)))
    response.headers["X-TIME-TO-EXECUTE"]=total
    return response


#Getting the highest budget title for the given year and country.
@app.route('/titles/<country>/<int:year>',methods=['GET'])
@token_required
def generate_highest_budget_titles(country,year):
    start=time.time()
    try:
        response=dynamodb.generate_highest_budget_titles(country,year)
    except:
        response= jsonify({'message' : 'Invalid request'})
    end=time.time()
    total=int(round(1000*(end-start)))
    response.headers["X-TIME-TO-EXECUTE"]=total
    return response


#Login for authentication to get the "X-ACCESS-TOKEN".
@app.route('/login')
def login():
    auth=request.authorization
    if auth and auth.password=='password':
        token=jwt.encode({'user':auth.username}, app.config['SECRET_KEY'])
        return jsonify({'token' : token})
    return make_response('Could not verify', 401, {'WWW-Authenticate' : 'Basic realm="Login required!"'})


if __name__ == '__main__':
    #Background Scheduler to sync the data every 60 minutes.
    job = scheduler.add_job(sync, 'interval', minutes=60)
    scheduler.start()

    app.run(host='127.0.0.1', port=5000, debug=True, use_reloader=False)