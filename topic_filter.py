###START-CONF
##{
##"object_name": "topic_filter",
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
##                      "state" : "ENGLISH"
##                  }
##              ],
##"return": [
##              {
##                      "name": "tweet",
##                      "description": "topic detector",
##                      "required": true,
##                      "type": "TweetString",
##                      "format": "",
##                      "state" : "MOVIE"
##                  }
##
##          ] }
##END-CONF

import re, os, time, cPickle
import urllib2
from random import randint
from pumpkin import PmkSeed
from nltk.corpus import reuters, movie_reviews
from operator import itemgetter
import nltk, pickle

class topic_filter(PmkSeed.Seed):

    def __init__(self, context, poi=None):
        PmkSeed.Seed.__init__(self, context,poi)
        self.wd = self.context.getWorkingDir()
        self.cache = []

    def on_load(self):
        print "Loading: " + self.__class__.__name__
        url = "https://www.dropbox.com/s/qn6o8r3liq5jxv4/topic_detection_data.pickle?dl=1"
        file_name = self.wd+"topic_detection_data.pickle"
        self.get_net_file(url, file_name)
        self.td = TopicDetector(file_name)

    def get_net_file(self, url, file_name):
        #file_name = url.split('/')[-1]
        downloaded = False
        while not downloaded:
            try:
                u = urllib2.urlopen(url)
                f = open(file_name, 'wb')
                meta = u.info()
                file_size = int(meta.getheaders("Content-Length")[0])
                self.logger.info ("Downloading: %s Bytes: %s" % (file_name, file_size))

                file_size_dl = 0
                block_sz = 8192
                while True:
                    buffer = u.read(block_sz)
                    if not buffer:
                        break

                    file_size_dl += len(buffer)
                    f.write(buffer)
                f.close()
                downloaded = True
            except Exception as e:
                self.logger.error("Error downloading, trying again....")
                time.sleep(5)
                pass

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
                if self.td.is_topic('movies', tw):
                    # self.logger.info("topic_filter: topic found in " + tw)
                    self.process_message(pkt, t, "MOVIE")


class TopicDetector:
    def __init__(self, path_to_data=None):
        self._load_vector(path_to_data)
        self.words = map(itemgetter(0), self.vector)
        self.topics = ["movies"]

    def _load_vector(self, path_to_data):
        if not path_to_data:
            path_to_data = '/tmp/stats.pickle'
        self.vector = None
        try:
            data_file = open(path_to_data, 'rb')
            self.vector = pickle.load(data_file)
        except IOError:
            data_file = open(path_to_data, 'wb')

            all_words = nltk.FreqDist(w.lower() for w in movie_reviews.words() if len(w) > 3)
            all_words_r = nltk.FreqDist(w.lower() for w in reuters.words() if len(w) > 3)

            self.vector = []

            for word in all_words.keys():
                ratio = 0
                try:
                    ratio = all_words.freq(word) / all_words_r.freq(word)
                except ZeroDivisionError:
                    next
                self.vector.append((word, ratio))
                self.vector.sort(key=itemgetter(1), reverse=True)
                self.vector = self.vector[:200]

            pickle.dump(self.vector, data_file)
    def is_topic(self, topic, text):
        if topic not in self.topics:
            None # todo: more topics than movies

        words = set([word.lower() for word in nltk.wordpunct_tokenize(text)])
        inter = words.intersection(self.words)
        return len(inter) > 1

