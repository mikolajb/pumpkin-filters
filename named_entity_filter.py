###START-CONF
##{
##"object_name": "named_entity_filter",
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
##                      "state" : "MOVIE"
##                  }
##              ],
##"return": [
##              {
##                      "name": "tweet",
##                      "description": "named entity extractor",
##                      "required": true,
##                      "type": "TweetString",
##                      "format": "",
##                      "state" : "ENTITIES"
##                  }
##
##          ] }
##END-CONF

import re, os, time, cPickle
import urllib2
from random import randint
from pumpkin import PmkSeed
from nltk import sent_tokenize, word_tokenize, pos_tag, ne_chunk

class named_entity_filter(PmkSeed.Seed):

    def __init__(self, context, poi=None):
        PmkSeed.Seed.__init__(self, context,poi)
        self.wd = self.context.getWorkingDir()
        self.cache = []

    def on_load(self):
        print "Loading: " + self.__class__.__name__

    def extract_named_entities(self, text):
        try:
            sentences = sent_tokenize(text)
            sentences = [word_tokenize(sent) for sent in sentences]
            sentences = [pos_tag(sent) for sent in sentences]
            result = []
            for sent in sentences:
                result += [word[0] for word, tag in ne_chunk(sent, binary=True).pos()
                           if tag == 'NE']
            return result
        except:
            self.logger.warning("exception in extract_named_entities")
            return []

    def process_message(self, pkt, message, category):
        self.cache.append(message)
        if len(self.cache) > 2000:
            self.dispatch(pkt, cPickle.dumps(self.cache), category)
            self.cache = []

    def run(self, pkt, tweet):
        tweet = cPickle.loads(tweet)
        for t in tweet:
            m = re.search('W(\s+)(.*)(\n)', t, re.S)
            if m:
                tw = m.group(2)
                self.logger.info("named_entity_filter: " + tw)
                entities = self.extract_named_entities(tw)
                if len(entities) > 0:
                    # self.logger.info("named_entity_filter: |" + "| ".join(entities))
                    self.process_message(pkt, ", ".join(entities), 'ENTITIES')

