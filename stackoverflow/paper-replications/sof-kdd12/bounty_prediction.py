"""
Purpose: Dataset for predicting wheather a question was sufficiently answered.

Description: Let A_k be the set of non-bounty question where k answers arrived
before one was accepted, where questioner did not had the privilage to offer a
bounty, meaning questioners reputation was less than 75.
Let B_k be the set of questions where a bounty was set after k answers arrived.
The prediction task is to predict given a random question, which among the above
mentioned two class does the question belongs to.

Filters:
1) Take questions with numbers of answers >= k (This is applied in sql)
2) From non-bounty questions remove all with questioners with reputation >= 75
(This is applied in dataframe)

Features:
Sa:
    * questioner reputation,
    * # of questioner’s questions
    * # of questioner’s answers
Sb:
    * # favorites on question
    * maximum answer score
    * maximum answerer reputation
    * positive question votes
    * negative question votes
Sc:
    * average answerer reputation
    * # positive votes on last answer
    * # negative votes on 2nd answer
    * length of highest-scoring answer
    * length of answer given by highest-reputation answerer
    * # comments on highest-scoring answer
Sd:
    * average time difference between answers
    * time difference between last 2 answers
    * time-rank of highest-scoring answer
    * time-rank of answer by highest-reputation answerer

Note: Need to record time of k-th arriving answer and also the (k+1)-th
arriving answer if any. If the 'Accepted Vote' creation time is between those
two times then that question belongs to set A_k. If the 'Bounty Vote' ceation
time is between those two times then that question belongs to set B_k. If
(k+1)-th answer is not available will consider its time to be infinity.

Steps:
1) Load posts, users and votes table into memory as global variables.
2) Write four seperate functions to generate the features and write to a csv.
3) Write another function to merge those four csv into one based on the question
id.
4) Merge the questions and answers as global too, its needed for all components.

**Refer to the stackoverflow documentation for all constant values used in the
code (e.g. posttypeid==1)
"""
import pandas as pd
import numpy as np
import mysql.connector
import time
import datetime
import logging
import sys

"""
Set value of k, default is set to three (used in the paper), or can be passed
as argument.
"""
k = 3 if len(sys.argv) < 2 else sys.argv[1]

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
users = pd.read_sql_query("select id,\
                                  accountid,\
                                  reputation\
                           from susers", mydb);
votes = pd.read_sql_query("select postid,\
                                  votetypeid,\
                                  unix_timestamp(creationdate) as votedate\
                           from votes", mydb);
posts = pd.read_sql_query("select id,\
                                  answercount,\
                                  commentcount,\
                                  unix_timestamp(creationdate) as creationdate,\
                                  owneruserid,\
                                  favoritecount,\
                                  parentid,\
                                  acceptedanswerid,\
                                  posttypeid,\
                                  score,\
                                  viewcount,\
                                  postlength\
                           from posts", mydb)
logging.info("Data retrieved ... Time passed %s", (datetime.datetime.now()-starttime))

votes['votedate'] = votes['votedate'].apply(lambda t: datetime.datetime.fromtimestamp(t))
posts['creationdate'] = posts['creationdate'].apply(lambda t: datetime.datetime.fromtimestamp(t))

"""
Merge the accountid and reputation with each post in the beginning, those are
the only two information we require from users table. Doing it once in the start
is better.
"""
posts = pd.merge(posts, users[['accountid', 'reputation']], how='left', left_on='owneruserid', right_on='accountid')
posts = posts.drop(columns=['accountid'])
"""
Get the count of positive and negative votes for each posts. Then merge with the
posts.
"""
upvotes = votes[votes['votetypeid'] == 2][['postid', 'votetypeid']]
downvotes = votes[votes['votetypeid'] == 3][['postid', 'votetypeid']]
posvotes = upvotes.groupby('postid', as_index=False).count()
posvotes.columns = ['id', 'posvotes']
negvotes = downvotes.groupby('postid', as_index=False).count()
negvotes.columns = ['id', 'negvotes']

posts = pd.merge(posts, posvotes, how='left', on='id')
posts = pd.merge(posts, negvotes, how='left', on='id')
posts['posvotes'] = posts['posvotes'].fillna(0)
posts['negvotes'] = posts['negvotes'].fillna(0)
"""
Seperate the posts into its two subgroups, questions and answers. Only include
questions with atleast k answer.
"""
questions = posts[(posts['posttypeid'] == 1) & (posts['answercount'] >= k)]
answers = posts[posts['posttypeid'] == 2]
"""
Add two new column on the questions to show its bounty status and its arrival
time.
"""
bounty = votes[votes['votetypeid'] == 8][['postid', 'votetypeid', 'votedate']]
bounty = bounty.sort_values('votedate').drop_duplicates('postid')
bounty.columns = ['id', 'hasbounty', 'bountydate']

questions = pd.merge(questions, bounty, how='left', on='id')
questions.loc[questions['hasbounty'].notnull(), 'hasbounty'] = 'yes'
questions.loc[questions['hasbounty'].isnull(), 'hasbounty'] = 'no'
"""
Add id answer is accepted to answers.
"""
accepted = votes[votes['votetypeid'] == 1][['postid', 'votetypeid', 'votedate']]
accepted = accepted.sort_values('votedate').drop_duplicates('postid')
accepted.columns = ['id', 'isaccepted', 'accepteddate']

answers = pd.merge(answers, accepted, how='left', on='id')
answers.loc[answers['isaccepted'].notnull(), 'isaccepted'] = 'yes'
answers.loc[answers['isaccepted'].isnull(), 'isaccepted'] = 'no'
"""
remove questions that do not have a bounty or an accepted answer
then remove questions with accepted answer where questioners reputation is >= 75
"""
questions = questions[(questions['hasbounty']=='yes') | ((questions['acceptedanswerid']!=0) & (questions['reputation']<75))]
"""
drop answers for questions not considered
"""
answers = answers[answers['parentid'].isin(questions['id'])]
"""
divide questions into bounty and non-bounty
"""
bounty_questions = questions[questions['hasbounty']=='yes']
nonbounty_questions = questions[questions['hasbounty']=='no']
"""
Follow the filter mentioned in the top note for both bounty and nonbounty
questions.
"""
# for bounty
# table with k and k+1 answer time
bounty_k_qa = pd.merge(bounty_questions[['id', 'creationdate', 'bountydate']], answers[['parentid', 'creationdate']], how='left', left_on='id', right_on='parentid', suffixes=['_q','_a'])
bounty_k_qa['delay'] = (bounty_k_qa['creationdate_a']-bounty_k_qa['creationdate_q']).astype('timedelta64[s]')
bounty_k_qa['timerank'] = bounty_k_qa.groupby('id')['delay'].rank(method='dense')
bounty_k_qa = bounty_k_qa[bounty_k_qa['timerank'].isin([3,4])]
btrk = bounty_k_qa[bounty_k_qa['timerank']==k]
btrkplus1 = bounty_k_qa[bounty_k_qa['timerank']==k+1]
bounty_k = pd.merge(btrk[['id', 'creationdate_a', 'bountydate']], btrkplus1[['id', 'creationdate_a']], how='left', on='id', suffixes=['_3', '_4'])
bounty_k['creationdate_a_4'] = bounty_k['creationdate_a_4'].fillna(bounty_k['bountydate']+datetime.timedelta(days=1))
bounty_k = bounty_k[(bounty_k['creationdate_a_3']<=bounty_k['bountydate']) & (bounty_k['bountydate']<bounty_k['creationdate_a_4'])]
bounty_questions = bounty_questions[bounty_questions['id'].isin(bounty_k['id'])]
# for nonbounty
nonbounty_k_qa = pd.merge(nonbounty_questions[['id', 'creationdate']], answers[['parentid', 'creationdate', 'accepteddate']], how='left', left_on='id', right_on='parentid', suffixes=['_q','_a'])
nonbounty_k_qa['delay'] = (nonbounty_k_qa['creationdate_a']-nonbounty_k_qa['creationdate_q']).astype('timedelta64[s]')
nonbounty_k_qa['timerank'] = nonbounty_k_qa.groupby('id')['delay'].rank(method='dense')
bounty_k_qa = nonbounty_k_qa[nonbounty_k_qa['timerank'].isin([3,4])]
nbtrk = bounty_k_qa[bounty_k_qa['timerank']==k]
nbtrkplus1 = bounty_k_qa[bounty_k_qa['timerank']==k+1]
nonbounty_k = pd.merge(nbtrk[['id', 'creationdate_a', 'accepteddate']], nbtrkplus1[['id', 'creationdate_a']], how='left', on='id', suffixes=['_3', '_4'])
nonbounty_k['creationdate_a_4'] = nonbounty_k['creationdate_a_4'].fillna(nonbounty_k['accepteddate']+datetime.timedelta(days=1))
nonbounty_k = nonbounty_k[(nonbounty_k['creationdate_a_3']<=nonbounty_k['accepteddate']) & (nonbounty_k['accepteddate']<nonbounty_k['creationdate_a_4'])]
nonbounty_questions = nonbounty_questions[nonbounty_questions['id'].isin(nonbounty_k['id'])]
"""
As the number of nonbounty_questions is much higher than bounty_questions, we
take a random sample of number of records from nonbq and get the features.
"""
nonbounty_questions = nonbounty_questions.sample(n=bounty_questions.shape[0], random_state=1)
"""
Concat both and merge with corresponding answers
"""
qa = pd.concat([nonbounty_questions, bounty_questions])
qa = pd.merge(qa, answers, how='left', left_on='id', right_on='parentid', suffixes=['_q', '_a'])
"""
Calculate delay time
"""
qa['delay'] = (qa['creationdate_a']-qa['creationdate_q']).astype('timedelta64[s]')
"""
From delay get their timeranks
"""
qa['timerank'] = qa.groupby('id_q')['delay'].rank(method='dense')
"""
Drop the answers with timerank > k+1
"""
qa_k = qa[qa['timerank'] <= k+1]
"""
Calcualte the timegap between answers for the remaining answers
"""
qa_k['answertimegap'] = qa_k.sort_values(['id_q', 'delay']).groupby('id_q')['delay'].diff()
qa_k['answertimegap'] = qa_k['answertimegap'].fillna(0)
"""
Write the 4 function to get the features from above 2 sets.
"""

def Sa():
    x = qa_k[['id_q', 'owneruserid_q', 'reputation_q', 'hasbounty']].drop_duplicates('id_q')
    x.columns = ['id', 'owneruserid', 'reputation', 'hasbounty']
    noq = pd.merge(users[['accountid']], x[['owneruserid']], how='inner', left_on='accountid', right_on='owneruserid').groupby('accountid', as_index=False).agg({'owneruserid':'count'})
    noq.columns = ['accountid', 'noq']
    noa = pd.merge(users[['accountid']], answers[['owneruserid']], how='inner', left_on='accountid', right_on='owneruserid').groupby('accountid', as_index=False).count()
    noa.columns = ['accountid', 'noa']
    sa = pd.merge(noq, noa, how='left', on='accountid')
    sa = pd.merge(x, sa, how='left', left_on='owneruserid', right_on='accountid')
    sa = sa.drop(columns=['owneruserid', 'accountid'])
    sa['noa'] = sa['noa'].fillna(0)
    return sa

def Sb():
    x = qa_k[['id_q', 'score_a', 'reputation_a', 'favoritecount_q', 'posvotes_q', 'negvotes_q']]
    max_score_row = x[['id_q', 'score_a', 'favoritecount_q', 'posvotes_q', 'negvotes_q']].sort_values('score_a', ascending=False).drop_duplicates(['id_q'])
    max_score_row.columns = ['id', 'maximum_answer_score', 'favorite_count', 'positive_question_votes', 'negative_question_votes']
    max_reputation_answerer = x[['id_q', 'reputation_a']].sort_values('reputation_a', ascending=False).drop_duplicates(['id_q'])
    max_reputation_answerer.columns = ['id', 'maximum_answerer_reputation']
    sb = pd.merge(max_score_row, max_reputation_answerer, how='left', on='id')
    sb.fillna(0, inplace=True)
    return sb

def Sc():
    x = qa_k[['id_q', 'timerank', 'score_a', 'reputation_a', 'commentcount_a', 'postlength_a', 'posvotes_a', 'negvotes_a']]

    last_answer_posvote = x[['id_q', 'timerank', 'posvotes_a']].sort_values('timerank', ascending=False).drop_duplicates('id_q')[['id_q', 'posvotes_a']]
    last_answer_posvote.columns = ['id', 'posvotes_last_answer']
    second_answer_negvote = x[['id_q', 'timerank', 'negvotes_a']][x['timerank']==2].drop_duplicates('id_q')[['id_q', 'negvotes_a']]
    second_answer_negvote.columns = ['id', 'negvotes_second_answer']
    max_score_row = x.sort_values('score_a', ascending=False).drop_duplicates(['id_q'])[['id_q', 'postlength_a', 'commentcount_a']]
    max_score_row.columns = ['id', 'max_score_answer_length', 'max_score_comment_count']
    max_reputation_row = x.sort_values('reputation_a', ascending=False).drop_duplicates(['id_q'])[['id_q', 'postlength_a']]
    max_reputation_row.columns = ['id', 'max_reputation_answer_length']
    stats = x[['id_q', 'reputation_a']].groupby('id_q', as_index=False).agg({'reputation_a':['mean']})
    stats.columns = ['id', 'mean_reputation']

    sc = pd.merge(max_score_row, max_reputation_row, how='left', on='id')
    sc = pd.merge(sc, last_answer_posvote, how='left', on='id')
    sc = pd.merge(sc, second_answer_negvote, how='left', on='id')
    sc = pd.merge(sc, stats, how='left', on='id')
    return sc

def Sd():
    x = qa_k[['id_q', 'answertimegap', 'delay', 'timerank', 'score_a', 'reputation_a']]

    stats = x[['id_q', 'answertimegap']].groupby('id_q', as_index=False).agg({'answertimegap':['mean']})
    stats.columns = ['id', 'mean_answertimegap']
    last_answer_timegap = x[['id_q', 'timerank', 'answertimegap']].sort_values('timerank', ascending=False).drop_duplicates('id_q')[['id_q', 'answertimegap']]
    last_answer_timegap.columns = ['id', 'time_diff_last_2_answers']
    max_score_row = x.sort_values('score_a', ascending=False).drop_duplicates(['id_q'])[['id_q', 'timerank']]
    max_score_row.columns = ['id', 'max_score_timerank']
    max_reputation_row = x.sort_values('reputation_a', ascending=False).drop_duplicates(['id_q'])[['id_q', 'timerank']]
    max_reputation_row.columns = ['id' ,'max_reputation_answer_timerank']

    sd = pd.merge(max_score_row, max_reputation_row, how='left', on='id')
    sd = pd.merge(sd, stats, how='left', on='id')
    sd = pd.merge(sd, last_answer_timegap, how='left', on='id')
    return sd

def merge_features():
    data = pd.merge(Sb(), Sa(), how='left', on='id')
    data = pd.merge(data, Sc(), how='left', on='id')
    data = pd.merge(data, Sd(), how='left', on='id')
    return data

if __name__ == '__main__':
    data = merge_features()
    print(data.head())
    data.to_csv("prediction2v1.csv")
