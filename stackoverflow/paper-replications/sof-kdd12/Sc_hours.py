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
# posts = pd.read_sql_query("select id, answercount, commentcount, unix_timestamp(creationdate) as creationdate, owneruserid, parentid, posttypeid, score, viewcount, postlength from posts limit 100", mydb)
posts = pd.read_sql_query("select id, answercount, commentcount, unix_timestamp(creationdate) as creationdate, owneruserid, parentid, acceptedanswerid, posttypeid, score, viewcount, postlength from posts where creationdate between \'2009-12-01\' and \'2009-12-31\'", mydb)
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
"""
qa = pd.merge(questions, answers, how='left', left_on='id', right_on='parentid', suffixes=['_q', '_a'])
# drop null reputations
qa = qa.dropna(subset=['reputation_a'])
qa['delay'] = (qa['creationdate_a']-qa['creationdate_q']).astype('timedelta64[s]')

t = [1,3,24,72]

mxs = []

for i in t:
	x = qa[qa['delay'] <= i*3600][['id_q', 'score_a', 'reputation_a', 'commentcount_a', 'postlength_a']]

	max_score_row = x.sort_values('score_a', ascending=False).drop_duplicates(['id_q'])[['id_q', 'postlength_a', 'commentcount_a']]
	max_score_row.columns = ['id', 'max_score_answer_length', 'max_score_comment_count']
	max_reputation_row = x.sort_values('reputation_a', ascending=False).drop_duplicates(['id_q'])[['id_q', 'postlength_a', 'commentcount_a']]
	max_reputation_row.columns = ['id', 'max_reputation_answer_length','max_reputation_answer_comment_count']
	stats = x[['id_q', 'reputation_a']].groupby('id_q', as_index=False).agg({'reputation_a':['mean', 'median', 'sum', 'max']})
	stats.columns = ['id', 'mean_reputation', 'median_reputation', 'sum_reputation', 'max_reputation']
	stats['max_reputation_contribution_to_sum'] = stats['max_reputation']/stats['sum_reputation']

	data = pd.merge(max_score_row, max_reputation_row, how='left', on='id')
	data = pd.merge(data, stats, how='left', on='id')
	data = data.drop(columns=['max_reputation'])
	data.to_csv('Sc_%dhour.csv'%(i), index=False)