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
votes = pd.read_sql_query("select postid, votetypeid from votes limit 100", mydb);
# posts = pd.read_sql_query("select id, answercount, owneruserid, parentid, favoritecount, posttypeid, score, viewcount from posts limit 100", mydb)
posts = pd.read_sql_query("select id, answercount, commentcount, unix_timestamp(creationdate) as creationdate, owneruserid, parentid, acceptedanswerid, posttypeid, score, viewcount, postlength from posts where creationdate between \'2009-12-01\' and \'2009-12-31\'", mydb)
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
3) Get all upvotes.
4) Get all downvotes.
"""
questions = posts[(posts['posttypeid'] == 1) & (posts['answercount'] > 0)]
logging.info("Total questions found %s", questions.shape)
answers = posts[posts['posttypeid'] == 2]
logging.info("Total answers found %s", answers.shape)
upvotes = votes[votes['votetypeid'] == 2]
logging.info("Total upvotes found %s", upvotes.shape)
downvotes = votes[votes['votetypeid'] == 3]
logging.info("Total downvotes found %s", downvotes.shape)


"""
Merge questions ans answers to perform comparisons.
Get everything with max score
1) sort qa and drop duplicates
"""
qa = pd.merge(questions, answers, how='left', left_on='id', right_on='parentid', suffixes=['_q', '_a'])


max_score_row = qa.sort_values('score_a', ascending=False).drop_duplicates(['id_q'])[['id_q', 'score_a', 'reputation_a', 'favoritecount_q', 'viewcount_q', 'answercount_q']]
max_score_row.columns = ['id', 'max_score', 'max_score_answerer_reputation', 'favoritecount', 'pageview', 'answercount']
max_reputation_answerer = qa.sort_values('reputation_a', ascending=False).drop_duplicates(['id_q'])[['id_q', 'reputation_a']]
max_reputation_answerer.columns = ['id', 'max_reputation_answerer']

pos_votes = upvotes.groupby('postid', as_index=False).count()
pos_votes.columns = ['id', 'pos_votes']
neg_votes = downvotes.groupby('postid', as_index=False).count()
neg_votes.columns = ['id', 'neg_votes']


"""
Final Merging
# """
data = pd.merge(max_score_row, max_reputation_answerer, how='left', on='id')
data = pd.merge(data, pos_votes, how='left', on='id')
data = pd.merge(data, neg_votes, how='left', on='id')
data.fillna(0, inplace=True)

print(data.iloc[:5,:])

# logging.info("All query complete ... Time passed %s", (datetime.datetime.now()-starttime))

# data.to_csv('Sb.csv', index=False)

# logging.info("Data saved to file, done... Time passed %s", (datetime.datetime.now()-starttime))