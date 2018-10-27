import pandas as pd 
import numpy as np 
import mysql.connector
import time
import datetime
import logging
import matplotlib.pyplot as plt
import sys

pd.set_option('display.width', 2000)
pd.set_option('display.max_columns', 50)
starttime = datetime.datetime.now()
logging.basicConfig(level=logging.INFO, filename=sys.argv[0].split('.')[0]+'.log')

mydb = mysql.connector.connect(
  host="localhost",
  user="root",
  passwd="password",
  database="stackoverflow2017"
)

logging.info("Starting to retreive data ... Time passed %s", (datetime.datetime.now()-starttime))
users = pd.read_sql_query("select id, accountid, reputation from users", mydb);
votes = pd.read_sql_query("select postid, votetypeid from votes where creationdate between \'2017-06-01\' and \'2017-12-31\'", mydb);
posts = pd.read_sql_query("select id, answercount, commentcount, unix_timestamp(creationdate) as creationdate, owneruserid, favoritecount, parentid, posttypeid, score, viewcount from posts where creationdate between \'2017-06-01\' and \'2017-12-31\' and answercount <= 5", mydb)
logging.info("Data retrieved ... Time passed %s", (datetime.datetime.now()-starttime))

data = None

"""
Attach reputaion with posts
1) Merge posts with users.
"""
posts = pd.merge(posts, users[['accountid', 'reputation']], how='left', left_on='owneruserid', right_on='accountid')
# posts = posts[posts['reputation'] > 10 ]

# posts = posts[posts['reputation'] < posts['reputation'].quantile(0.99)]

"""
Start filtering posts.
1) Get all question with at least one answer.
2) Get all answers.
"""
questions = posts[(posts['posttypeid'] == 1) & (posts['answercount'] > 0)]
logging.info("Total questions found %s", questions.shape)
answers = posts[posts['posttypeid'] == 2]
logging.info("Total answers found %s", answers.shape)
upvotes = votes[votes['votetypeid'] == 2]
logging.info("Total upvotes found %s", upvotes.shape)
# downvotes = votes[votes['votetypeid'] == 3]
# logging.info("Total downvotes found %s", downvotes.shape)

perpost_upvotes = upvotes.groupby('postid', as_index=False).count()
perpost_upvotes.columns = ['id', 'votecount']
answers = pd.merge(answers, perpost_upvotes, how='left', on='id')



"""
Merge questions ans answers to perform comparisons.
"""
qa = pd.merge(questions, answers, how='left', left_on='id', right_on='parentid', suffixes=['_q', '_a'])

"""
Get everything with max score
1) sort qa and drop duplicates
"""
max_score_row = qa.sort_values('score_a', ascending=False).drop_duplicates(['id_q'])[['id_q', 'score_a', 'favoritecount_q', 'answercount_q', 'viewcount_q', 'votecount']]
max_score_row.columns = ['id', 'max_score', 'favoritecount', 'answercount', 'viewcount', 'votecount']
# max_score_row = max_score_row[max_score_row['favoritecount']]
max_5 = max_score_row[max_score_row['max_score']==5]

nof = []
for i in range(1, 7):
	s = max_5[max_5['answercount']==i]
	print(s.shape)
	nof.append(max_5[max_5['answercount']==i]['favoritecount'].mean())

 
plt.plot(range(1,7), nof)
plt.xlabel("# of answers on questions", fontsize=18)
plt.ylabel("Avg # of favorites", fontsize=18)
plt.show()
print(nof)



# max_5.to_csv('fig7.csv', index=False)
# print(max_5.iloc[:10,:])
# max_reputation_row = qa.sort_values('reputation_a', ascending=False).drop_duplicates(['id_q'])[['id_q', 'postlength_a', 'commentcount_a']]
# max_reputation_row.columns = ['id', 'max_reputation_answer_length','max_reputation_answer_comment_count']
# stats = qa[['id_q', 'reputation_a']].groupby('id_q', as_index=False).agg({'reputation_a':['mean', 'median', 'sum', 'max']})
# stats.columns = ['id', 'mean_reputation', 'median_reputation', 'sum_reputation', 'max_reputation']
# stats['max_reputation_contribution_to_sum'] = stats['max_reputation']/stats['sum_reputation']

# """
# Final Merging
# """
# data = pd.merge(max_score_row, max_reputation_row, how='left', on='id')
# data = pd.merge(data, stats, how='left', on='id')

# print(data.iloc[:5, :])

# logging.info("All query complete ... Time passed %s", (datetime.datetime.now()-starttime))

# data.to_csv('Sc.csv', index=False)

# logging.info("Data saved to file, done... Time passed %s", (datetime.datetime.now()-starttime))