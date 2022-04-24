import re
from pyspark.sql import SparkSession, Row

from nltk import word_tokenize
import nltk

from datetime import datetime, timezone
import string
import json

import pickle
import nltk
import snowflake.connector as sf
import config
import tensorflow

from keras.models import load_model
from tensorflow.keras.preprocessing.sequence import pad_sequences

from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import functions as F
from textblob import TextBlob

http_regex = "@\S+|https?:\S+|http?:\S|[^A-Za-z0-9]+"
extended_stopwords_path = './models/extendedStopwords.txt'

topics = ["vaccinationdrive",
          "administration",
          "sideeffects",
          "efficacy"]

TABLE_NAME = 'ARCHIVE_1'

def convertDatetime(str):
    try:
        new_datetime = datetime.strptime(str, '%d-%m-%y %H:%M')
        return int(new_datetime.timestamp())
    except ValueError:
        pass
        
    try:
        new_datetime = datetime.strptime(str, '%d/%m/%y %H:%M')
        return int(new_datetime.timestamp())
    except ValueError:
        pass

    try:
        new_datetime = datetime.strptime(str, '%d.%m.%y %H:%M')
        return int(new_datetime.timestamp())
    except ValueError:
        pass

    return 0

def cleanseText(text):

    # 1. change to lowercase and strip whitespace
    text = text.lower().strip()

    # 2. remove '\n'
    text = text.replace('\n', ' ')

    # 3. remove emojis
    text = text.encode('ASCII', 'ignore').decode()

    # 4. remove url
    text = re.sub(http_regex, ' ', text).strip()

    # 5. remove stopwords
    text = removeStopWords(text)

    # 6. remove multiple whitespaces
    while '  ' in text:
        text = text.replace('  ', ' ')

    return text

def filterDate(text):
    if text == None: 
        return False

    try:
        datetime.strptime(text, '%Y-%m-%d %H:%M:%S')
        return True
    except ValueError:
        pass

    try:
        datetime.strptime(text, '%d-%m-%Y %H:%M')
        return True
    except ValueError:
        pass

    try:
        datetime.strptime(text, '%d/%m/%Y %H:%M')
        return True
    except ValueError:
        pass

    try:
        datetime.strptime(text, '%d.%m.%Y %H:%M')
        return True
    except ValueError:
        pass

    return False

def removeStopWords(text):
    from nltk.corpus import stopwords

    tokenizedTweet = word_tokenize(text)

    stopWords = stopwords.words('english')

    with open(extended_stopwords_path) as f:
        extended_stopwords = f.readlines()

    for i in range(len(extended_stopwords)):
        extended_stopwords[i] = extended_stopwords[i].replace('\n', '')

    newTweet = ''
    for word in tokenizedTweet:
        if word not in stopWords and word not in extended_stopwords:
            if word in string.punctuation:
                newTweet = newTweet.strip() + word + ' '
            else:
                newTweet += word + ' '
    return newTweet.strip()

def generateTopicTerms():
  path = "filter_words.json"
  file = open(path)
  topic_terms = json.load(file)
  for topic in topics:
    topic_terms[topic] = set(topic_terms[topic])

  return topic_terms

def createTopicRow(row, topic_terms):
    tweet = row.tweet
    if tweet == None or tweet == '':
        return []

    words = tweet.split(" ")
    result = []
    for t in topics:
        for w in words:
            if w in topic_terms[t]:
                result.append(
                    Row(topic=t, time=row.date, user=row.user, tweet=tweet, terms=list(topic_terms[t])))
                break
    return result

def sentiment_analyse(text):
    ### SENTIMENT
    #Pads and tokenizes tweets for input into the model. Padding value is 0 by default
    #Output should look like: [[0,0,0,0,...0,32,43,5,432],...[0,0,...0,4234,554,43]]
    #Use pre-padding because LSTMs are somewhat biased to the end of an input.
    #Tweet size limit is 280 chars, avg english word is 4.7 chars. Set padding to 70 to capture full tweet 90% of the time [citation needed]
    input_text = pad_sequences(tokenizer.texts_to_sequences([text]), padding='pre', maxlen=70)
    p = model.predict(input_text)[0][0]
    if p > 0.6:
        return 1
    elif p > 0.4:
        return 0
    else:
        return -1

def createTable():
    cnx = sf.connect(user = config.username, 
                        password = config.password,
                        account = config.account, 
                        warehouse = config.warehouse,
                        database = config.database)
    cursor = cnx.cursor()
    
    sfTableName = '{}.{}.{}'.format(config.database, config.schema, TABLE_NAME)
    
    TABLES = {}
    TABLES[sfTableName] = (
            '''
            CREATE TABLE {} (
            id INTEGER NOT NULL AUTOINCREMENT,
            posted_at datetime NOT NULL, 
            post text NOT NULL,
            topic text NOT NULL,
            sentiment integer NOT NULL,
            PRIMARY KEY (id) );
            '''.format(sfTableName)
            )
    for tableName in TABLES:
        tableDesc = TABLES[tableName]
        try:
            cursor.execute(
                '''
                DROP TABLE {};
                '''.format(tableName)
            )
            cursor.execute(tableDesc)
            print('Created {}'.format(tableName))
        except Exception as err:
            print(err)
    cnx.close()

def convert_datetime(time):
    try:
        return datetime.strptime(time, '%Y-%m-%d %H:%M:%S')
    except ValueError:
        pass

    try:
        return datetime.strptime(time, '%d-%m-%Y %H:%M')
    except ValueError:
        pass

    try:
        return datetime.strptime(time, '%d/%m/%Y %H:%M')
    except ValueError:
        pass

    try:
        return datetime.strptime(time, '%d.%m.%Y %H:%M')
    except ValueError:
        pass
    
    return datetime.now()


def insertTable(row):
    cnx = sf.connect(user = config.username, 
                        password = config.password,
                        account = config.account, 
                        warehouse = config.warehouse,
                        database = config.database)
    cursor = cnx.cursor()
    
    sfTableName = '{}.{}.{}'.format(config.database, config.schema, TABLE_NAME)
    
    #Create the insert command dynamically based on the packet
    #Packet is a dict, the dict key is the field name, the value at the dict key is the value... obviously
    insertCommand = 'INSERT INTO {} ('.format(sfTableName)
    packet = {'posted_at' : convert_datetime(row['time']), 'post' : row['tweet'], 'topic' : row['topic'], 'sentiment' : row['sentiment']}
    for field in packet:
        insertCommand += '{}, '.format(field)
    insertCommand = insertCommand[0:-2] + ') VALUES (%('
    for field in packet:
        insertCommand += '{})s, %('.format(field)
    insertCommand = insertCommand[0:-4] + ')'
    #End result:
    #'INSERT INTO topic_dbType (field1, ..., fieldn) VALUES (%(field1)s, ..., %(fieldn)s)'
    cursor.execute(insertCommand, packet)
    cnx.commit()
    cnx.close()

def polarity_detection(text):
    p = TextBlob(text).sentiment.polarity
    if p > 0:
        return 1
    elif p == 0:
        return 0
    else:
        return -1

def subjectivity_detection(text):
    return TextBlob(text).sentiment.subjectivity

if __name__ == '__main__':
    path = "data/covidvaccine.csv"

    spark = SparkSession \
        .builder \
        .master("local[1]") \
        .appName("Twitter Data Cleanser") \
        .getOrCreate()

    df = spark.read \
        .option("header", True) \
        .option("multiline", "true") \
        .csv(path)

    df = df \
        .drop(col("user_location")) \
        .drop(col("user_description")) \
        .drop(col("user_created")) \
        .drop(col("user_followers")) \
        .drop(col("user_favourites")) \
        .drop(col("user_verified")) \
        .drop(col("user_friends")) \
        .drop(col("source")) \
        .drop(col("is_retweet")) \
        .drop(col("hashtags")) \

    # udf to lowercase the text
    cleanseTextUDF = udf(lambda x: cleanseText(x), StringType())

    # udf to filter wrong date format
    filterDateUDF = udf(lambda x: filterDate(x), BooleanType())

    df = df \
        .filter(col("text").isNotNull()) \
        .filter(filterDateUDF(col("date"))) \
        .withColumn('text', cleanseTextUDF(col("text"))) \
        .withColumnRenamed('text', 'tweet') \
        .withColumnRenamed('user_name', 'user') \
        .withColumn('topic', lit(None).cast(StringType())) \
        .withColumn('terms', lit(None).cast(StringType()))

    df.show()

    topic_terms = generateTopicTerms()

    df = df.rdd.flatMap(lambda x: createTopicRow(x, topic_terms)).toDF()

    df.show()

    # this model is generalized sentiment analysis trained on sentiment140 from kaggle
    # model = load_model('./models/TwitSent.h5')
    # with open('./models/Toke_TwittSent.pkl', 'rb') as file:
    #     tokenizer = pickle.load(file)
    # print("load model success")

    # sentiment_analyse_udf = udf(sentiment_analyse, IntegerType())
    # df = df.withColumn("sentiment", sentiment_analyse_udf("tweet"))
    polarity_analyse_udf = udf(polarity_detection, IntegerType())
    df = df.withColumn("sentiment", polarity_analyse_udf("tweet"))
    df.show()

    createTable()

    cnx = sf.connect(user = config.username, 
                        password = config.password,
                        account = config.account, 
                        warehouse = config.warehouse,
                        database = config.database)
    cursor = cnx.cursor()
    
    sfTableName = '{}.{}.{}'.format(config.database, config.schema, TABLE_NAME)
    
    #Create the insert command dynamically based on the packet
    #Packet is a dict, the dict key is the field name, the value at the dict key is the value... obviously
    insertCommand = 'INSERT INTO {} ('.format(sfTableName)
    fields = ['posted_at', 'post', 'topic', 'sentiment']
    for field in fields:
        insertCommand += '{}, '.format(field)
    insertCommand = insertCommand[0:-2] + ') VALUES (%('
    for field in fields:
        insertCommand += '{})s, %('.format(field)
    insertCommand = insertCommand[0:-4] + ')'
    #End result:
    #'INSERT INTO topic_dbType (field1, ..., fieldn) VALUES (%(field1)s, ..., %(fieldn)s)'
    for row in df.collect():
        packet = {'posted_at' : convert_datetime(row['time']), 'post' : row['tweet'], 'topic' : row['topic'], 'sentiment' : row['sentiment']}
        cursor.execute(insertCommand, packet)
        print('Inserted the following row {}'.format(packet))
    cnx.commit()
    cnx.close()    
    spark.stop()
