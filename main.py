from functions import *
import os

user = os.environ['USER']
host = os.environ['HOST']
# port = os.environ['PORT']
port = '5432'
db = os.environ['DB']
pw = os.environ['PW']

db_url = os.environ['DATABASE_URL']

api_key = os.environ['API_KEY']
api_secret = os.environ['API_SECRET']
ax_token = os.environ['ACCESS_TOKEN']
ax_token_secret = os.environ['ACCESS_TOKEN_SECRET']

schema, table = 'coins', 'cmc_base_recently_added'
conn = db_connection(user=user, host=host, port=port, db=db, cred=pw)

df = cmc_recently_added()

if __name__ == '__main__':
    execute_tweet(df=df,
                  schema=schema,
                  table=table,
                  conn=conn,
                  akey=api_key,
                  asecret=api_secret,
                  axtoken=ax_token,
                  axsecret=ax_token_secret
                  )
