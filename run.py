#Run this automatically with a crontab job

# Updating the database with new adult url.
import mysql.connector

# website retrieval
from bs4 import BeautifulSoup as soup
from bs4.element import Comment
import requests
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
from konlpy.tag import Okt

ssl._create_default_https_context = ssl._create_unverified_context
annotator = VnCoreNLP(address="http://127.0.0.1", port=9000)

kParse = Okt()

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
    print('attempting to querry'+ url)
    try:
        response = requests.get(url, timeout=2)
        response.raise_for_status()
    except Exception as err:
        print(f'Error for: {url} occured')
        return (), 'zz'
    else:
        html = response.text
        text = text_from_html(html)
        if(len(text) < 100):
            return (), 'zz'
        lang = detect(text) 
        #tokenizing text

        # THEN WE NEED TO MAKE DIFFERENT LOOPS DEPENDING ON THE LANGUAGE OF THE TEXT
    
        text = re.sub(r'[^\w\s]','',text)

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
        elif lang == 'ko':
            output = kParse.morphs(text)
        else: 
            print("skipping because uknown language")
            return (), 'zz'
        stopL = set(stopwords(lang))
        out = [w for w in output if not w in stopL]

        #setting directory
        return tuple(output), lang



# Connects to the local mysql here and automatically commits anything.
lconn = mysql.connector.connect(host='', user='',passwd='') #Enter in host, username, and password for your local mysql database.
lconn.autocommit = True
lcur = lconn.cursor(buffered=True)
print('connected')


lcur.execute('USE cfs;')

#Querrying
    
print('Retrieiving Data:')
lcur.execute("SELECT url, yn, id FROM adttable WHERE parsed = 0 AND yn = 'Y' limit 2000;") #Change depending on how powerful your server or computer is, ensure it can run the script inside an hour.
urlq = lcur.fetchall()
print('Data Retrieved.')

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
updatetext = "UPDATE adttable SET parsed = 1 WHERE id = %s;"
for x in urlq:
    stem, lang = urlParze(x[0])
    if len(stem) == 0:
        print('moving on')
    else:
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
    lcur.execute(updatetext, (x[2],))
    print(x[0] + "finished")
jsonCloser(enPath, enDict)
jsonCloser(idPath, idDict)
jsonCloser(thPath, thDict)
jsonCloser(viPath, viDict)
jsonCloser(koPath, koDict)
jsonCloser(aPath, aDict)

print('Update Complete, Success')
