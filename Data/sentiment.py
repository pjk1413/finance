
# Import libraries
from urllib.error import HTTPError
from urllib.request import urlopen, Request
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.common.keys import Keys
import os
import pandas as pd
import Interface.utility as utility
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
from Database.crud_func import crud
from Data.config_read import config as get_values
from Database.database import database
import mysql.connector as connect
import math
import time

class sentiment:
    def __init__(self):
        config = get_values()
        self.conn = database().conn_sentiment
        self.news_tables = {}
        # self.tickers = ['VRCA', 'AAPL', 'SG', 'SFT', 'PBTS', 'IMAC', 'HOLI', 'AACG']
        # https://chromedriver.chromium.org/downloads
        self.tickers = crud().get_list_of_stocks()
        self.finwiz_url = config.finwiz_url
        self.time = time


    def search_result_analyzer(self):
        driver = webdriver.Chrome()
        driver.get("https://www.google.com/finance")
        # assert "Python" in driver.title
        search = driver.find_element_by_id("search-bar")
        search.send_keys("GOOG")
        search.send_keys(Keys.RETURN)

        results = driver.find_elements_by_class_name("yY3Lee")

        for result in results:
            print("RESULT")
            text = result.find_element_by_class_name("AoCdqe")
            print(text.get_attribute("innerText"))

        # assert "No results found." not in driver.page_source
        # driver.close()



    def sentiment_analysis(self, parsed_news):
        # Instantiate the sentiment intensity analyzer
        # pip install vaderSentiment
        vader = SentimentIntensityAnalyzer()
        # Set column names
        columns = ['ticker', 'date', 'time', 'headline', 'link']
        # Convert the parsed_news list into a DataFrame called 'parsed_and_scored_news'

        parsed_and_scored_news = pd.DataFrame(parsed_news, columns=columns)

        # Iterate through the headlines and get the polarity scores using vader
        scores = parsed_and_scored_news['headline'].apply(vader.polarity_scores).tolist()

        # Convert the 'scores' list of dicts into a DataFrame
        scores_df = pd.DataFrame(scores)

        # Join the DataFrames of the news and the list of dicts
        parsed_and_scored_news = parsed_and_scored_news.join(scores_df, rsuffix='_right')

        # Convert the date column from string to datetime
        parsed_and_scored_news['date'] = pd.to_datetime(parsed_and_scored_news.date).dt.date
        # parsed_and_scored_news.head()
        return parsed_and_scored_news


    # https://towardsdatascience.com/sentiment-analysis-of-stocks-from-financial-news-using-python-82ebdcefb638
    def gather_headlines(self):

        l = len(self.tickers)
        utility.printProgressBar(0, l, prefix='Progress:', suffix='Complete', length=50)
        for z, ticker in enumerate(self.tickers):
            response = None
            data = None
            url = self.finwiz_url + "=" + ticker[0]

            try:

                req = Request(url=url,headers={'user-agent': 'my-app/0.0.1'})
                response = urlopen(req)
                # Read the contents of the file into 'html'
                html = BeautifulSoup(response, features='lxml')

                # Find 'news-table' in the Soup and load it into 'news_table'
                news_table = html.find(id='news-table')

                parsed_news = []

                if news_table == None:
                    continue
                for x in news_table.findAll('tr'):
                    # read the text from each tr tag into text
                    # get text from a only
                    text = x.a.get_text()
                    link = x.a['href']
                    # splite text in the td tag into a list
                    date_scrape = x.td.text.split()
                    # if the length of 'date_scrape' is 1, load 'time' as the only element

                    if len(date_scrape) == 1:
                        time = date_scrape[0]

                    # else load 'date' as the 1st element and 'time' as the second
                    else:
                        date = date_scrape[0]
                        time = date_scrape[1]

                    # Append ticker, date, time and headline as a list to the 'parsed_news' list
                    parsed_news.append([ticker, date, time, text, link])


            except:
                self.time.sleep(1)


            data = self.sentiment_analysis(parsed_news)
            self.insert_data(data)
            utility.printProgressBar(z + 1, l, prefix='Progress:', suffix='Complete', length=50)

    def insert_data(self, data):
        # Organize data, make sure it will fit into table
        for i in range(data.shape[0]):
            date = data.iloc[i].get('date')
            time = data.iloc[i].get('time')
            ticker = data.iloc[i].get('ticker')
            headline = str(data.iloc[i].get('headline')).replace("'", "\'")[:255]
            link = data.iloc[i].get('link')
            neg = self.flt_num(data.iloc[i].get('neg'))
            neu = self.flt_num(data.iloc[i].get('neu'))
            pos = self.flt_num(data.iloc[i].get('pos'))
            compound = self.flt_num(data.iloc[i].get('compound'))

            exists = self.check_if_exists(date, time, ticker, headline, link)
            values = (date, headline, link, neg, neu, pos, compound)
            if exists:
                continue
            else:
                try:
                    sql_statement = f"INSERT INTO {ticker[0]}_SENT (dt, headline, url, sent_neg, sent_neutral, sent_pos, sent_compound) " \
                                    "VALUES (%s, %s, %s, %s, %s, %s, %s)"
                    cursor = self.conn.cursor()
                    cursor.execute(sql_statement, values)
                    self.conn.commit()
                except connect.errors as err:
                    print(err)


    def check_if_exists(self, date, time, ticker, headline, link):
        # returns 1 if exists, 0 if not
        sql_statement = f"SELECT IF( EXISTS( SELECT * FROM {ticker[0]}_SENT WHERE dt = '{date}' AND url = '{link}'), 1, 0)";

        cursor = self.conn.cursor(buffered=True)
        data = None
        try:
            cursor.execute(sql_statement)
            exists = cursor.fetchone()
        except connect.errors as error:
            print(error)

        if exists[0] == 1:
            return True
        else:
            return False


    def flt_num(self, number):
        if math.isnan(number):
            return float('NaN')
        else:
            return round(float(number),3)