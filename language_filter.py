###START-CONF
##{
##"object_name": "language_filter",
##"object_poi": "qpwo-2345",
##"auto-load": true,
##"remoting" : true,
##"parameters": [
##                 {
##                      "name": "tweet",
##                      "description": "",
##                      "required": true,
##                      "type": "TweetString",
##                      "format": "",
##                      "state" : "RAW"
##                  }
##              ],
##"return": [
##              {
##                      "name": "tweet",
##                      "description": "topic detector",
##                      "required": true,
##                      "type": "TweetString",
##                      "format": "",
##                      "state" : "ENGLISH"
##                  }
##
##          ] }
##END-CONF

import re, os, time, cPickle
import urllib2
import nltk
from random import randint
from pumpkin import PmkSeed
from nltk import wordpunct_tokenize
from nltk.corpus import stopwords

class language_filter(PmkSeed.Seed):

    def __init__(self, context, poi=None):
        PmkSeed.Seed.__init__(self, context,poi)
        self.wd = self.context.getWorkingDir()
        self.cache = []

    def on_load(self):
        print "Loading: " + self.__class__.__name__
	wd = self.context.getWorkingDir()
        nltk.data.path.append(wd + "nltk_data")

    def detect_language(self, text):

        words = [word.lower() for word in wordpunct_tokenize(text)]
        result = (None, -1)

        for language in stopwords.fileids():
            stopwords_set = set(stopwords.words(language))
            words_set = set(words)
            common_elements = words_set.intersection(stopwords_set)
            ratio = float(len(common_elements)) / len(stopwords_set)

            if ratio > result[1]:
                result = (language, ratio)

        return result[0]

    def process_message(self, pkt, message, category):
        self.cache.append(message)
        if len(self.cache) > 2000:
            self.dispatch(pkt, cPickle.dumps(self.cache), category)
            self.cache = []

    def run(self, pkt, tweet):
        tweet = cPickle.loads(str(tweet))
        for t in tweet:
            m = re.search('W(\s+)(.*)(\n)', t, re.S)
            if m:
                tw = m.group(2)
                if len(tw) > 10:
                    language = self.detect_language(tw)
                    if language == 'english':
                        self.process_message(pkt, t, 'ENGLISH')

