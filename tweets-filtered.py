
import requests
import credentials
import json

import pandas as pd
import sqlalchemy
from sqlalchemy import create_engine

from pandas.io.json import json_normalize

import urllib3
import psycopg2


def auth():
    bearer_token = credentials.bearer_token
    return bearer_token


def create_url():
    return "https://api.twitter.com/2/tweets/sample/stream"


def create_headers(bearer_token):
    headers = {"Authorization": "Bearer {}".format(bearer_token)}
    return headers


payload = {
    #   "expansions": "attachments.poll_ids,attachments.media_keys,author_id,entities.mentions.username,geo.place_id,in_reply_to_user_id,referenced_tweets.id,referenced_tweets.id.author_id",
    "tweet.fields": "created_at,id,lang,entities,public_metrics",
    #   "user.fields": "description",
    # "media.fields": "url",
    # "place.fields": "country_code",
    # "poll.fields": "options"
}


def connect_to_endpoint(url, headers):
    response = requests.request("GET", url, headers=headers, stream=True, params=payload)
    print(response.status_code)

    for response_line in response.iter_lines():
        if response_line:
            try:

                data = json.loads(response_line)

                textmaybewithouthashtag = json.loads(response_line)["data"]["text"]
                df = json_normalize(data)
                df2 = pd.DataFrame(df)
                df2 = df2[['data.created_at', 'data.id']]
                engine = create_engine('postgresql+psycopg2://postgres:warriors@localhost:5432/stocks')

                df2.to_sql("alltweets", engine, if_exists='append', dtype={'relevant_column': sqlalchemy.types.JSON})
                #                 print("alltweets loaded")

                # checks for hashtags
                if "entities" in data["data"]:
                    #                     print("The key is present.\n")
                    a = data["data"]["entities"].get('hashtags')
                    if a is not None:
                        res = [sub['tag'] for sub in a]
                        for i in res:
                            hashtags = i
                            idfinal = data["data"]["id"]
                            textwithhashtag = data["data"]["text"]
                            createdatfinal = data["data"]["created_at"]
                            like_count = data["data"]["public_metrics"]["like_count"]
                            retweet_count = data["data"]["public_metrics"]["retweet_count"]
                            quote_count = data["data"]["public_metrics"]["quote_count"]
                            reply_count = data["data"]["public_metrics"]["reply_count"]

                            keywords = (
                            'stock market', 'wall street', 'day trading', 'stocks', 'daily stocks', 'stocks2020',
                            'investing stocks', 'TSLA', 'APPL', 'NIFTY', 'NASDAQ', 'stockslogic',
                            'StocksLogicPortfolio',
                            'midcap50', 'Trading101', 'investing', 'trading', 'money', 'investment', 'finance', 'forex',
                            'investor', 'business', 'invest', 'financialfreedom'
                            , 'trader', 'wallstreet', 'entrepreneur', 'bitcoin', 'stock', 'sharemarket', 'daytrader',
                            'forextrader', 'nifty', 'daytrading'
                            , 'wealth', 'cryptocurrency', 'stockmarketnews', 'forextrading', 'sensex', 'warrenbuffet',
                            'intraday', 'swingtrading',
                            'stockmarketindia', 'forexsignals', 'niftyfifty', 'stock markets', 'intraday trading',
                            'investing tips', 'stockmarketeducation',
                            'banknifty')
                            #                             keywords=('biden','Biden','election2020','trump','riggedelection','kamla harris')
                            if any(keyword in hashtags.lower() for keyword in keywords) or any(
                                    keyword in textwithhashtag for keyword in keywords):

                                #                                 finaldf = {'id': [idfinal],'text': [textwithhashtag],'created_At': [createdatfinal],'hashtags': hashtags
                                #                                           ,'Like_count':[like_count],'Quote_count':[quote_count],'Reply_count':[reply_count],'Retweet_count':[retweet_count]}

                                finaldf = [[idfinal, textwithhashtag, createdatfinal, hashtags, like_count, quote_count,
                                            reply_count, retweet_count]]
                                dffinal = pd.DataFrame(finaldf,
                                                       columns=['id', 'text', 'created_At', 'hashtags', 'Like_count',
                                                                'Quote_count', 'Reply_count', 'Retweet_count'])

                                engine = create_engine(
                                    'postgresql+psycopg2://postgres:warriors@localhost:5432/stocks')

                                dffinal.to_sql("filtered-tweets", engine, if_exists='append',
                                               dtype={'relevant_column': sqlalchemy.types.JSON})

                                print("Match loaded")
                            else:
                                print("no match")

                else:

                    keywords = ('stock market', 'wall street', 'day trading', 'stocks', 'daily stocks', 'stocks2020',
                                'investing stocks', 'TSLA', 'APPL', 'NIFTY', 'NASDAQ', 'stockslogic',
                                'StocksLogicPortfolio',
                                'midcap50', 'Trading101', 'investing', 'trading', 'money', 'investment', 'finance',
                                'forex', 'investor', 'business', 'invest', 'financialfreedom'
                                , 'trader', 'wallstreet', 'entrepreneur', 'bitcoin', 'stock', 'sharemarket',
                                'daytrader', 'forextrader', 'nifty', 'daytrading'
                                , 'wealth', 'cryptocurrency', 'stockmarketnews', 'forextrading', 'sensex',
                                'warrenbuffet', 'intraday', 'swingtrading',
                                'stockmarketindia', 'forexsignals', 'niftyfifty', 'stock markets', 'intraday trading',
                                'investing tips', 'stockmarketeducation',
                                'banknifty')
                    #                     keywords=('biden','Biden','election2020','trump','riggedelection','kamla harris')
                    if any(keyword in textmaybewithouthashtag for keyword in keywords):
                        idfinal = data["data"]["id"]
                        hashtags = "NULL"
                        createdatfinal = data["data"]["created_at"]
                        like_count = data["data"]["public_metrics"]["like_count"]
                        retweet_count = data["data"]["public_metrics"]["retweet_count"]
                        quote_count = data["data"]["public_metrics"]["quote_count"]
                        reply_count = data["data"]["public_metrics"]["reply_count"]

                        finaldf2 = [
                            [idfinal, textmaybewithouthashtag, createdatfinal, hashtags, like_count, quote_count,
                             reply_count, retweet_count]]
                        dffinal = pd.DataFrame(finaldf2, columns=['id', 'text', 'created_At', 'hashtags', 'Like_count',
                                                                  'Quote_count', 'Reply_count', 'Retweet_count'])

                        engine = create_engine(
                            'postgresql+psycopg2://postgres:warriors@localhost:5432/stocks')

                        dffinal.to_sql("filtered-tweets", engine, if_exists='append',
                                       dtype={'relevant_column': sqlalchemy.types.JSON})







            except IOError as io:
                print("ERROR!")

    if response.status_code != 200:
        raise Exception(
            "Request returned an error: {} {}".format(
                response.status_code, response.text
            )
        )


def main():
    bearer_token = auth()
    url = create_url()
    headers = create_headers(bearer_token)
    timeout = 0
    while True:
        connect_to_endpoint(url, headers)
        timeout += 1


# cc created,atid,text
if __name__ == "__main__":
    main()
