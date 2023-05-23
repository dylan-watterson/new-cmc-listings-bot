from sqlalchemy import create_engine, text
from selenium import webdriver
from selenium.webdriver.support.ui import Select
from bs4 import BeautifulSoup
from datetime import datetime
import time
import pandas as pd
import tweepy
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart


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


def email_error(project, schema, conn, auth, sender, recipient):
    """
    Sends an email alerting potential error within the app.

    :param project: name of project / app.
    :param schema: schema name to add error.
    :param conn: postgres connector.
    :param auth: email account credentials.
    :return: sends error alert via email.
    """

    q = f"select date_sent from {schema}.email_log where project = '{project}'"
    date_sent = pd.read_sql(text(q), con=conn.connect())

    if date_sent.empty:
        date_sent = ''
    else:
        date_sent = date_sent.iloc[0]['date_sent']

    today = datetime.now().date()

    if today != date_sent:
        # Set up the message headers
        msg = MIMEMultipart()
        msg['From'] = sender
        msg['To'] = recipient
        msg['Subject'] = f'Error Alert for {project}'

        # Set up the message body
        body = f'There is an error within the app - {project}.'
        msg.attach(MIMEText(body, 'plain'))

        # Set up the SMTP server
        server = smtplib.SMTP('smtp.gmail.com', 587)
        server.starttls()
        server.login(sender, auth)

        # Send the email
        text_msg = msg.as_string()
        server.sendmail(sender, recipient, text_msg)
        server.quit()

        d = {
            'project': [project],
            'date_sent': [today]
        }
        df = pd.DataFrame(data=d)

        df.to_sql('email_log', con=conn, schema=schema, if_exists='replace')

    return


def web_crawler(path, options):
    """
    Uses Selenium and bs4 to scrape all wwe superstars on the current roster.

    :param path: os path to selenium driver.
    :return: DataFrame - list of current superstars
    """

    # Create a WebDriver instance
    chrome_options = webdriver.ChromeOptions()
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--no-sandbox")
    print(path, options)
    chrome_options.binary_location = options

    driver = webdriver.Chrome(executable_path=path, chrome_options=chrome_options)

    # Navigate to the webpage
    url = 'https://www.wwe.com/superstars'  # Replace with the URL of the webpage you want to scrape
    driver.get(url)

    # Locate the dropdown element
    dropdown = driver.find_element('id', 'superstar-search-select')

    # Create a Select instance for the dropdown
    select = Select(dropdown)

    # Dropdown selection
    select.select_by_visible_text('Current Superstars')

    # Scroll down repeatedly until the end of the page is reached
    SCROLL_PAUSE_TIME = 3

    # Get scroll height
    last_height = driver.execute_script("return document.body.scrollHeight")

    while True:
        print(last_height)

        # Scroll down to bottom
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")

        # Wait to load page
        time.sleep(SCROLL_PAUSE_TIME)

        # Calculate new scroll height and compare with last scroll height
        new_height = driver.execute_script("return document.body.scrollHeight")
        if new_height == last_height:
            break
        last_height = new_height

    # Get the HTML content of the dynamically loaded page
    html = driver.page_source

    # Parse the HTML content using BeautifulSoup
    soup = BeautifulSoup(html, 'html.parser')

    # Find all current names
    names = soup.find_all('span', {'class': 'superstars--name'})
    print(names)

    # # Get the current date and time
    # current_datetime = datetime.now()
    #
    # # Format the current date and time as 'YYYY-MM-DD HH:MM'
    # formatted_datetime = current_datetime.strftime('%Y-%m-%d %H:%M')

    current_names = []
    for name in names:
        superstar = name.text.strip()
        current_names.append(superstar)

    d = {
        'current_superstars': current_names
        # ,
        # 'scraped_datetime': formatted_datetime
    }

    df = pd.DataFrame(data=d)

    # Close the WebDriver
    driver.close()

    return df


def create_added_tweet(txt):
    """
    Creates tweet for an added wwe superstar.

    :param txt:
    :return:
    """

    # Green tick check mark
    check_mark = 'U00002705'
    tweet = (
        f"""{chr(int(check_mark[1:], 16))} {txt} added to Current Superstars!\n"""
        f"""#wwe #wweraw #wwesmackdown #wwesd #wwenxt #{txt.replace(' ', '').replace("'", "")}"""
    )
    print('Executing Tweet!\n')
    print(tweet)
    return tweet


def create_removed_tweet(txt):
    """
    Creates tweet for a removed wwe superstar.

    :param txt:
    :return:
    """

    # Red cross X check mark
    check_mark = 'U0000274C'
    tweet = (
        f"""{chr(int(check_mark[1:], 16))} {txt} removed from Current Superstars!\n"""
        f"""#wwe #wweraw #wwesmackdown #wwesd #wwenxt #{txt.replace(' ', '').replace("'", "")}"""
    )
    print('Executing Tweet!\n')
    print(tweet)
    return tweet


def execute_tweet(df, schema, table, conn, akey, asecret, axtoken, axsecret):
    """

    :param df: list of current wwe superstars
    :param schema: schema name
    :param table: table name
    :param conn:
    :param akey:
    :param asecret:
    :param axtoken:
    :param axsecret:
    :return:
    """

    q = f"select * from information_schema.tables where table_schema = '{schema}' and table_name = '{table}'"
    init_table = pd.read_sql(text(q), con=conn.connect())
    print(init_table)

    if init_table.empty:
        df.to_sql('wwe_current_base_table', con=conn, schema=schema, if_exists='replace', index=False)

    # Check for added wwe superstars
    base_query = "select current_superstars from wwe.wwe_current_superstar_table EXCEPT select current_superstars " \
                 "from wwe.wwe_current_base_table "
    base_check = pd.read_sql(text(base_query), con=conn.connect())

    # Check for removed wwe superstars
    current_query = "select current_superstars from wwe.wwe_current_base_table EXCEPT select current_superstars from " \
                    "wwe.wwe_current_superstar_table "
    current_check = pd.read_sql(text(current_query), con=conn.connect())

    # Creates tweet is a new value has been added to Current Superstars.
    if not base_check.empty:
        for c in base_check['current_superstars']:

            # # API keys that yous saved earlier
            api_key = akey
            api_secrets = asecret
            access_token = axtoken
            access_secret = axsecret

            # # Authenticate to Twitter
            auth = tweepy.OAuthHandler(api_key, api_secrets)
            auth.set_access_token(access_token, access_secret)

            api = tweepy.API(auth)

            try:
                api.verify_credentials()
                print('Successful Authentication')

            except:
                print('Failed authentication')

            tweet = create_added_tweet(txt=c)

            # Text Post
            status = tweet
            api.update_status(status=status)

        df.to_sql('wwe_current_base_table', con=conn, schema=schema, if_exists='replace')

    # Creates tweet is a value has been removed to Current Superstars.
    if not current_check.empty:
        for c in current_check['current_superstars']:

            # # API keys that yous saved earlier
            api_key = akey
            api_secrets = asecret
            access_token = axtoken
            access_secret = axsecret

            # # Authenticate to Twitter
            auth = tweepy.OAuthHandler(api_key, api_secrets)
            auth.set_access_token(access_token, access_secret)

            api = tweepy.API(auth)

            try:
                api.verify_credentials()
                print('Successful Authentication')

            except:
                print('Failed authentication')

            tweet = create_removed_tweet(txt=c)

            # Text Post
            status = tweet
            api.update_status(status=status)

        df.to_sql('wwe_current_superstar_table', con=conn, schema=schema, if_exists='replace')

    else:
        print('No superstars have been added/removed from Current!')

    return
