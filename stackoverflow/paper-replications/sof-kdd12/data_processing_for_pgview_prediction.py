import pandas as pd 
import numpy as np 
import mysql.connector
import time
import datetime
import logging
import sys

pd.set_option('display.width', 2000)
pd.set_option('display.max_columns', 50)
starttime = datetime.datetime.now()
logging.basicConfig(level=logging.INFO, filename=sys.argv[0].split('.')[0]+'.log')

mydb = mysql.connector.connect(
  host="localhost",
  user="root",
  passwd="password",
  database="stackoverflow2010"
)

logging.info("Starting to retreive data ... Time passed %s", (datetime.datetime.now()-starttime))
users = pd.read_sql_query("select id, accountid, reputation from susers", mydb);
posts = pd.read_sql_query("select id, answercount, commentcount, unix_timestamp(creationdate) as creationdate, owneruserid, parentid, posttypeid, score, viewcount, postlength from posts", mydb)
posts['creationdate'] = posts['creationdate'].apply(lambda t: datetime.datetime.fromtimestamp(t))
logging.info("Data retrieved ... Time passed %s", (datetime.datetime.now()-starttime))

data = None

"""
Attach reputaion with posts
1) Merge posts with users.
"""
posts = pd.merge(posts, users[['accountid', 'reputation']], how='left', left_on='owneruserid', right_on='accountid')

"""
Start filtering posts.
1) Get all question with at least one answer.
2) Get all answers.
"""
questions = posts[(posts['posttypeid'] == 1) & (posts['answercount'] > 0)]
logging.info("Total questions found %s", questions.shape)
answers = posts[posts['posttypeid'] == 2]
logging.info("Total answers found %s", answers.shape)

"""
Merge questions ans answers to perform comparisons.
1) Remove all answers after 30 days of question time.
"""
qa = pd.merge(questions, answers, how='left', left_on='id', right_on='parentid', suffixes=['_q', '_a'])
qa['delay'] = qa['creationdate_a']-qa['creationdate_q'] # Getting the delay time
# print(qa['delay'])
qa = qa[(qa['delay'].astype('timedelta64[s]')<=3600*24) | (qa['delay'].astype('timedelta64[s]')<0)] # Removing negtive and more than 30 days answers

"""
Get everything with max score
1) sort qa and drop duplicates
"""
max_score_row = qa.sort_values('score_a', ascending=False).drop_duplicates(['id_q'])[['id_q', 'postlength_a', 'delay', 'commentcount_a']]
max_score_row.columns = ['id', 'max_score_answer_length', 'max_score_answer_arrival', 'max_score_comment_count']
max_reputaion_comment = qa.sort_values('reputation_a', ascending=False).drop_duplicates(['id_q'])[['id_q', 'commentcount_a']]
max_reputaion_comment.columns = ['id', 'max_reputation_answerer_comment_count']


"""
1) Remove all question with no answer in the first hour.
"""

in_first_hour = qa[['id_q', 'delay', 'score_a']].groupby('id_q', as_index=False).agg({'delay':['min'], 'score_a':['sum']}) # get the min values
in_first_hour.columns = ['id', 'mindelay', 'sumscore']
in_first_hour = in_first_hour[(in_first_hour['mindelay'].astype('timedelta64[s]') <= 3600)] # Filter ids by values

questions_in_first_hour = pd.merge(questions, in_first_hour, how='inner', on='id')
"""
Merge questions ans users to get noq
1) Merge question ans users
"""
noq = pd.merge(users[['accountid']], questions[['owneruserid']], how='inner', left_on='accountid', right_on='owneruserid').groupby('accountid', as_index=False).count()
noq.columns = ['accountid', 'noq']

questions_in_first_hour = pd.merge(questions_in_first_hour, noq, how='left', left_on='owneruserid', right_on='accountid')

"""
Final Merging
"""
data = questions_in_first_hour[['id', 'answercount', 'viewcount', 'sumscore', 'noq', 'reputation']]
data = pd.merge(data, max_score_row, how='left', on='id')
data = pd.merge(data, max_reputaion_comment, how='left', on='id')

logging.info("All query complete ... Time passed %s", (datetime.datetime.now()-starttime))

data.to_csv('task1.csv')

logging.info("Data saved to file, done... Time passed %s", (datetime.datetime.now()-starttime))