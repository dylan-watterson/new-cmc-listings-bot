from functions import *

# user, server, port, db, schema = 'postgres', 'localhost', '5432', 'postgres', 'coins'
# Pull in API Key
cred_path = '/Users/dylanwatterson/Documents/my_repos/NewCoinListing/db_creds.json'
with open(cred_path, 'r') as j:
    creds = json.loads(j.read())

user, host, port, db, pw = creds['user'],  creds['host'], creds['port'], creds['db'], creds['password']
schema, table = 'coins', 'cmc_base_recently_added'

conn = db_connection(user=user, host=host, port=port, db=db, cred=pw)


df = cmc_recently_added()

if __name__ == '__main__':
    execute_tweet(df=df, schema=schema, table=table, conn=conn)

