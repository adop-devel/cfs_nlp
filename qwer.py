# Updating the database with new adult url.
import mysql.connector

# website retrieval
from bs4 import BeautifulSoup as soup
from bs4.element import Comment
import urllib.request as req
import urllib
import re
from pathlib import Path
import ssl

#tokenizer
import nltk
from stopwordsiso import stopwords
from nltk.tokenize import RegexpTokenizer
from nltk.stem import WordNetLemmatizer
import os
from langdetect import detect
from langdetect import DetectorFactory
import json
from pythainlp import word_tokenize as thaiword
from vncorenlp import VnCoreNLP
from nlp_id.lemmatizer import Lemmatizer as indLemm
import signal
import string
from itertools import chain

ssl._create_default_https_context = ssl._create_unverified_context
annotator = VnCoreNLP(address="http://127.0.0.1", port=9000)

def jsonOpener(langpath):
    try:
        with open(langpath) as json_file:
            urlList = json.load(json_file)
    except: 
        urlList = {}
    return urlList

def jsonCloser(langpath, dicts):
    with open(langpath, 'w', encoding='utf8') as outfile:
        json.dump(dicts, outfile, ensure_ascii=False)
    print(langpath + "succesfully written")

def handler(signum, frame, url):
    lcurr.execute("UPDATE adttable SET parsed 2 WHERE url = %s", url)

#methods
def querryF(select, create, insert):
    cur.execute(select)
    print('selected')
    data = cur.fetchall()
    lcur.execute(create)
    lcur.executemany(insert, data)
    print('Inserted Data into DB')

# Parses element of the html
def tag_visible(element):
    if element.parent.name in ['style', 'script', 'head', 'title', 'meta', '[document]']:
        return False
    if isinstance(element, Comment):
        return False
    return True

# retrieves text from the html
def text_from_html(body):
    souped = soup(body, 'html.parser')
    texts = souped.findAll(text=True)
    visible_texts = filter(tag_visible, texts)
    return u" ".join(t.strip() for t in visible_texts)

def urlParze(url):
    DetectorFactory.seed = 0
    html = req.urlopen(url).read()
    text = text_from_html(html)
    lang = detect(text) 
    #tokenizing text

    # THEN WE NEED TO MAKE DIFFERENT LOOPS DEPENDING ON THE LANGUAGE OF THE TEXT
    
    text = re.sub(r'[^\w\s]','',text)
    print(text)

    # if language is english or indonesian
    if lang == 'en' or lang == 'id':
        text = nltk.word_tokenize(text)
        lowered = [x.lower() for x in text]
        if lang == 'en':
            lemmatizer = WordNetLemmatizer()
            output = [lemmatizer.lemmatize(x) for x in lowered]
        if lang == 'id':
            indLem = indLemm()
            output = [indLem.lemmatize(x) for x in lowered]
    elif lang == 'th':
        output = thaiword(text, keep_whitespace=False)
    elif lang == 'vi':
        output = list(chain.from_iterable(annotator.tokenize(text)))
    #elif lang == 'ko':

    
    stopL = set(stopwords(lang))
    out = [w for w in output if not w in stopL]

    #setting directory
    return tuple(output), lang




# Connects with the online database
conn = mysql.connector.connect(host='', user='',passwd='') # Put in your passwords
cur = conn.cursor()

# Connects to the local mysql here and automatically commits anything.
lconn = mysql.connector.connect(host='localhost', user='cfs',passwd='adop')
lconn.autocommit = True
lcur = lconn.cursor(buffered=True)
print('connected')

adultQ = "SELECT adt_idx, adt_url, adt_url_md5, adt_reg_dt, adt_yn FROM i_adult_url LIMIT 100"
adultC = "CREATE TABLE IF NOT EXISTS adttable(id INT(11) NOT NULL, url VARCHAR(1024) NOT NULL, md5 CHAR(32), dt DATETIME, yn CHAR(1), parsed TINYINT(1) DEFAULT 0, PRIMARY KEY (id));"
adultI = "INSERT IGNORE INTO adttable(id, url, md5, dt, yn) VALUES (%s, %s, %s, %s, %s);"

cur.execute('USE insight;')
lcur.execute('USE cfs;')
choice = int(input("Would you like to 1. do a basic update 2. Run a full database update, or 3. "))

if choice == 1:

    #Querrying
    querryF(adultQ, adultC, adultI)
    cur.close()
    print('Data transfer complete')
    lcur.execute("SELECT url, yn FROM adttable WHERE parsed = 0 limit 5;")
    urlq = lcur.fetchall()

    #everything up to here works

    #Opening Json files
    enPath = 'token_data/enList.txt'
    idPath = 'token_data/idList.txt'
    thPath = 'token_data/thList.txt'
    viPath = 'token_data/viList.txt'
    koPath = 'token_data/koList.txt'
    aPath = 'token_data/ansList.txt'
    
    enDict = jsonOpener(enPath)
    idDict = jsonOpener(idPath)
    thDict = jsonOpener(thPath)
    viDict = jsonOpener(viPath)
    koDict = jsonOpener(koPath)
    aDict = jsonOpener(aPath)

    #Parsing found url list of words into json
    print('beginning Parsing to json files')

    #Remove after finished testing

    for x in urlq:

        #IMPORTANTNOTE, FIGURE OUT HOW TO DIRECT TO DIFFERENT LANGUES

        stem, lang = urlParze(x[0])

        # FInish putting this into a dictionary
        if lang == 'en':
            enDict[x[0]] = stem
        elif lang == 'id':
            idDict[x[0]] = stem
        elif lang == 'th':
            thDict[x[0]] = stem
        elif lang == 'vi':
            viDict[x[0]] = stem
        elif lang == 'ko':
            koDict[x[0]] = stem
        #put into answer key
        aDict[x[0]] = x[1]
        print (stem)
        # uncomment once finished testing
        # lcurr.execute("UPDATE adttable SET parsed 1 WHERE url = %s", x[0])

    jsonCloser(enPath, enDict)
    jsonCloser(idPath, idDict)
    jsonCloser(thPath, thDict)
    jsonCloser(viPath, viDict)
    jsonCloser(koPath, koDict)
    jsonCloser(aPath, aDict)

    print('Update Complete, Success')

elif choice == 2:
    print('choice number2')
elif choice == 3:
    print('Goodbye')


# Querries for adult_url

# Querries for stopword
# stopQ = "SELECT * FROM i_stopword"
# stopC = "CREATE TABLE IF NOT EXISTS stoptable (stopword_no SMALLINT(9) NOT NULL, keyword VARCHAR(50) CHARACTER SET utf8, reg VARCHAR(5), del_yn CHAR(1), dt DATETIME, ui_idx SMALLINT(6), PRIMARY KEY (stopword_no);"
# stopI = "INSERT IGNORE INTO stoptable(stopword_no, keyword, reg, del_YN, dt, ui_idx) VALUES (%s, %s, %s, %s, %s, %s);"



