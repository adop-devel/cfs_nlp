# CFS with Natural Language Processing and Machine Learning

## The goal of this project is to uitilize NLP and ML to create a model which will be able to classify websites as being appropriate or inappropriate without human intervention. This project was created to be somewhat modular for easy debugging and for multiple processes to be running at once.

1. Retrieve the URLs of websites marked as appropriate or inappropriate from the cloud database utilizing a mysql querry. 
2. Retrieve the contents of each website and take the elements as one large string.
3. Automatically determine the language of each website, and run the appropriate NLP algorithm for that specific language.
4. Each algorithm will split the string into separate words, remove punctuation and captilization (if it exists), and if possible, change verb stems so that it remains the root word.
5. Once this process is complete, it will output the list of words into a dictionary with the url as the key, according to its language.
6. Once they apply this to all urls, the dictionary will be exported as a json file for later use.

7. The machine learning script will take one of the dictionaries depending on the text analysis it wants to perform and extract the data
8. Performs tf_idf analysis and outputs a matrix with the correct matrix corresponding to the correct words.
9. The corresponding answer of whether or not the document is appropriate or inappropriate will be placed in a separate dictionary, with the same key as the matrix. 
10. It will run through the machine learning algorithm and output a model which can be used to classify other websites.

## Special notes:
vncorenlp -Xmx500m /home/ubuntu/cfs/VnCoreNLP/VnCoreNLP-1.1.1.jar -p 9000 -a "wseg"
must be run in one ubuntu instance before using runing the qwer.py script. You must open another instance in order to continue
