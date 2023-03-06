from sqlalchemy import create_engine, text
from bs4 import BeautifulSoup
import pandas as pd
import requests
import tweepy
import pyshorteners
import re


def db_connection(user, host, port, db, cred=''):
    """
    Postgres database connector.
    @param user: username.
    @param host: server name.
    @param port: connection port.
    @param db: database name.
    @param cred: password for user name. Default = ''.
    @return: conn
    """

    conn = create_engine(f'postgresql+psycopg2://{user}:{cred}@{host}:{port}/{db}?sslmode=require')
    return conn


def cmc_recently_added():
    """

    :return:
    """

    url = 'https://coinmarketcap.com/new/'

    user_agent = {'User-agent': 'Mozilla/5.0'}
    response = requests.get(url, headers=user_agent)

    soup = BeautifulSoup(response.text, 'html.parser')

    recent_coins = soup.find('table')
    names = soup.find_all('p', {'class': 'ePTNty'})
    symbols = soup.find_all('p', {'class': 'coin-item-symbol'})

    names = [name.text for name in names]
    symbols = [sym.text for sym in symbols]

    df_table = pd.read_html(str(recent_coins))
    df = df_table[0].drop(['Unnamed: 0', '#', 'Unnamed: 10'], axis=1)

    df['Name'] = names
    df['Symbol'] = symbols
    df = df[['Name', 'Symbol', 'Price', '1h', '24h', 'Fully Diluted Market Cap', 'Volume', 'Blockchain', 'Added']]

    old_cols = [c for c in df.columns]
    for c in df.columns:
        df[c.lower().replace(' ', '_')] = df[c]

    df = df.drop(old_cols, axis=1)
    return df


def create_tweet(df):
    """

    :param df:
    :return:
    """

    coin = df['name'].head(1)[0]
    sym = df['symbol'].head(1)[0]
    price = df['price'].head(1)[0]
    market = df['fully_diluted_market_cap'].head(1)[0]
    # volume = df['volume'].head(1)[0]
    chain = df['blockchain'].head(1)[0]
    # added = df['added'].head(1)[0]
    print(chain, str(chain))
    # create an instance of the Shortener class
    shortener = pyshorteners.Shortener()

    coin_url = re.sub(r'[\s\W_]+', '-', coin.lower())
    url = f"https://coinmarketcap.com/currencies/{coin_url}/"

    # shorten the URL
    short_url = shortener.tinyurl.short(url)
    print(short_url, len(short_url))

    hashtag_coin = re.sub(r'[\s\W_]+', '', coin)

    check_mark = 'U00002705'
    tweet = (
        f"""{chr(int(check_mark[1:], 16))} New Coin Added on Coin Market Cap!\n\n"""
        f"""Coin: {coin}\n"""
        f"""Symbol: {sym}\n"""
        f"""Current Price: {price}\n"""
        f"""Fully Diluted MC: {market}\n"""
        # f"""Volume: {volume}\n"""
        f"""Blockchain: {chain}\n"""#Added: {added}\n\n"""
        f"""#crypto #cryptocurrency #{hashtag_coin} #{sym} {'' if chain == 'Own Blockchain' else '#'+chain} #BTC \n"""
        f"""{short_url}"""
    )
    print('Executing Tweet!\n')
    print(tweet)
    return tweet


def execute_tweet(df, schema, table, conn, akey, asecret, axtoken, axsecret):
    """

    :param df:
    :param schema:
    :param table:
    :param conn:
    :param akey:
    :param asecret:
    :param axtoken:
    :param axsecret:
    :return:
    """

    q = f"select * from information_schema.tables where table_schema = '{schema}' and table_name = '{table}'"
    init_table = pd.read_sql(text(q), con=conn.connect())

    if init_table.empty:
        df.to_sql('cmc_base_recently_added', con=conn, schema=schema, if_exists='replace')

    df.to_sql('cmc_current_recently_added', con=conn, schema=schema, if_exists='replace')

    query = "select name from coins.cmc_current_recently_added EXCEPT select name from coins.cmc_base_recently_added"

    base_check = pd.read_sql(text(query), con=conn.connect())

    if not base_check.empty:
        for c in base_check['name']:
            dfx = df[df['name'] == c].reset_index().drop(['index'], axis=1)

            # # API keys that yous saved earlier
            api_key = akey
            api_secrets = asecret
            access_token = axtoken
            access_secret = axsecret

            # # # Authenticate to Twitter
            auth = tweepy.OAuthHandler(api_key, api_secrets)
            auth.set_access_token(access_token, access_secret)

            api = tweepy.API(auth)

            try:
                api.verify_credentials()
                print('Successful Authentication')

            except:
                print('Failed authentication')

            tweet = create_tweet(df=dfx)

            # Text Post
            status = tweet
            api.update_status(status=status)

        df.to_sql('cmc_base_recently_added', con=conn, schema=schema, if_exists='replace')
    else:
        print('No new coins added!')

    return
