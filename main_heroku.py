from wwe_functions import *
import os

start = datetime.now()

user = os.environ['USER']
host = os.environ['HOST']
port = '5432'
db = os.environ['DB']
pw = os.environ['PW']

db_url = os.environ['DATABASE_URL']

api_key = os.environ['API_KEY']
api_secret = os.environ['API_SECRET']
ax_token = os.environ['WWE_ACCESS_TOKEN']
ax_token_secret = os.environ['WWE_ACCESS_TOKEN_SECRET']

schema, table = 'wwe', 'wwe_current_base_table'
conn = db_connection(user=user, host=host, port=port, db=db, cred=pw)

df = web_crawler(path=os.environ.get("CHROMEDRIVER_PATH"), options=os.environ.get("GOOGLE_CHROME_BIN"))

try:
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
except:
    p = 'wwe-current-roster-bot'
    auth = os.environ['MAIL']
    sender = os.environ['SENDER']
    recipient = os.environ['RECIPIENT']
    email_error(project=p, schema=schema, conn=conn, auth=auth, sender=sender, recipient=recipient)

end = datetime.now() - start

print(f"Run Time: {str(end)}")
