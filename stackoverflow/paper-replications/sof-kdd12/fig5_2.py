import pandas as pd 
import numpy as np 
import mysql.connector
import time
import datetime
import logging
import sys
import matplotlib.pyplot as plt

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
votes = pd.read_sql_query("select postid, votetypeid from votes where creationdate between \'2017-01-01\' and \'2017-12-31\'", mydb);
# posts = pd.read_sql_query("select id, answercount, owneruserid, parentid, favoritecount, posttypeid, score, viewcount from posts limit 100", mydb)
posts = pd.read_sql_query("select id, answercount, unix_timestamp(creationdate) as creationdate, owneruserid, parentid, acceptedanswerid, posttypeid from posts where creationdate between \'2017-01-01\' and \'2017-12-31\'", mydb)
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
accepted = votes[votes['votetypeid'] == 1]

accepted = accepted.groupby('postid', as_index=False).count()
accepted.columns = ['id', 'accepted']

pos_votes = upvotes.groupby('postid', as_index=False).count()
pos_votes.columns = ['id', 'pos_votes']
neg_votes = downvotes.groupby('postid', as_index=False).count()
neg_votes.columns = ['id', 'neg_votes']

total_votes = votes.groupby('postid', as_index=False).count()
total_votes.columns = ['id', 'total_votes']

answers = pd.merge(answers, pos_votes, how='left', on='id')
answers = pd.merge(answers, neg_votes, how='left', on='id')
answers = pd.merge(answers, accepted, how='left', on='id')
answers = pd.merge(answers, total_votes, how='left', on='id')

answers[['pos_votes', 'neg_votes', 'accepted', 'total_votes']] = answers[['pos_votes', 'neg_votes', 'accepted', 'total_votes']].fillna(0)

answers['reputation_won'] = answers['pos_votes']/(answers['pos_votes']+answers['neg_votes'])



"""
Merge questions ans answers to perform comparisons.
Get everything with max score
1) sort qa and drop duplicates
# """
qa = pd.merge(questions, answers, how='left', left_on='id', right_on='parentid', suffixes=['_q', '_a'])
qa['delay'] = (qa['creationdate_a']-qa['creationdate_q']).astype('timedelta64[s]') # Getting the delay time
qa['timerank'] = qa.groupby('id_q')['delay'].rank(method='dense')
# qa = pd.merge(qa, pos_votes, )

print(qa.head())

q2 = qa[qa['answercount_q']==2].groupby('timerank', as_index=False).agg({'pos_votes': 'sum', 'neg_votes':'sum'})[['timerank', 'pos_votes', 'neg_votes']]
q2['2'] = q2['pos_votes']/(q2['pos_votes']+q2['neg_votes'])

q3 = qa[qa['answercount_q']==3].groupby('timerank', as_index=False).agg({'pos_votes': 'sum', 'neg_votes':'sum'})[['timerank', 'pos_votes', 'neg_votes']]
q3['3'] = q3['pos_votes']/(q3['pos_votes']+q3['neg_votes'])

q4 = qa[qa['answercount_q']==4].groupby('timerank', as_index=False).agg({'pos_votes': 'sum', 'neg_votes':'sum'})[['timerank', 'pos_votes', 'neg_votes']]
q4['4'] = q4['pos_votes']/(q4['pos_votes']+q4['neg_votes'])

q5 = qa[qa['answercount_q']==5].groupby('timerank', as_index=False).agg({'pos_votes': 'sum', 'neg_votes':'sum'})[['timerank', 'pos_votes', 'neg_votes']]
q5['5'] = q5['pos_votes']/(q5['pos_votes']+q5['neg_votes'])

q2 = pd.DataFrame({'x':range(1,3), '2':q2['2']})
q3 = pd.DataFrame({'x':range(1,4), '3':q3['3']})
q4 = pd.DataFrame({'x':range(1,5), '4':q4['4']})
q5 = pd.DataFrame({'x':range(1,6), '5':q5['5']})

print(q2)
print(q3)
print(q4)
print(q5)

plt.plot('x','2', data=q2, marker='o', markerfacecolor='red', markeredgecolor='black', markersize='12', color='red', linewidth=4)
plt.plot('x','3', data=q3, marker='s', markerfacecolor='blue', markeredgecolor='black', markersize='12', color='blue', linewidth=4)
plt.plot('x','4', data=q4, marker='d', markerfacecolor='green', markeredgecolor='black', markersize='12', color='green', linewidth=4)
plt.plot('x','5', data=q5, marker='^', markerfacecolor='yellow', markeredgecolor='black', markersize='12', color='yellow', linewidth=4)
# plt.plot('x','5', data=dfs[4], marker='v', markerfacecolor='cyan', markeredgecolor='black', markersize='12', color='cyan', linewidth=4)
plt.xticks(range(1,6))
plt.xlabel("Time-rank of answer", fontsize=18)
plt.ylabel("Fraction of positive votes", fontsize=18)
plt.legend()
plt.show()
# max_score_row = qa.sort_values('score_a', ascending=False).drop_duplicates(['id_q'])[['id_q', 'score_a', 'reputation_a', 'favoritecount_q', 'viewcount_q', 'answercount_q']]
# max_score_row.columns = ['id', 'max_score', 'max_score_answerer_reputation', 'favoritecount', 'pageview', 'answercount']
# max_reputation_answerer = qa.sort_values('reputation_a', ascending=False).drop_duplicates(['id_q'])[['id_q', 'reputation_a']]
# max_reputation_answerer.columns = ['id', 'max_reputation_answerer']



# """
# Final Merging
# # """
# data = pd.merge(max_score_row, max_reputation_answerer, how='left', on='id')
# data = pd.merge(data, pos_votes, how='left', on='id')
# data = pd.merge(data, neg_votes, how='left', on='id')
# data.fillna(0, inplace=True)

# print(data.iloc[:5,:])

# logging.info("All query complete ... Time passed %s", (datetime.datetime.now()-starttime))

# data.to_csv('Sb.csv', index=False)

# logging.info("Data saved to file, done... Time passed %s", (datetime.datetime.now()-starttime))