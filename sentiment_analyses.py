###START-CONF
##{
##"object_name": "sentiment_analyses",
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
##                      "description": "sentiment analysis",
##                      "required": true,
##                      "type": "TweetString",
##                      "format": "",
##                      "state" : "POSITIVE"
##                  }
##
##          ] }
##END-CONF

import re, os, time, cPickle
import urllib2
from random import randint
from pumpkin import PmkSeed
import nltk

class sentiment_analyses(PmkSeed.Seed):

    def __init__(self, context, poi=None):
        PmkSeed.Seed.__init__(self, context,poi)
        self.wd = self.context.getWorkingDir()
        self.cache = []

    def on_load(self):
        print "Loading: " + self.__class__.__name__
        nltk.data.path.append(self.wd)
        url = "https://github.com/mikolajb/soa-cloud-course/raw/master/assignment02/movie_reviews_NaiveBayes.pickle"
        file_name = self.wd+"movie_reviews_NaiveBayes.pickle"
        file = "movie_reviews_NaiveBayes.pickle"

        self.get_net_file(url, file_name)
        #os.chmod(file_name, 0777)
        self.classifier = nltk.data.load(file)

    def check(self, tweet):
        words = tweet.split()
        feats = dict([(word, True) for word in words])
        return self.classifier.classify(feats) == 'pos'

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
                    #status = r"%10d  [%3.2f%%]" % (file_size_dl, file_size_dl * 100. / file_size)
                    #status = status + chr(8)*(len(status)+1)
                    #print status,
                f.close()
                downloaded = True
            except Exception as e:
                self.logger.error("Error downloading, trying again....")
                time.sleep(5)
                pass



    def some_check(self, tweet):
        x = randint(0,1)
        if x == 0:
            return True
        else:
            return False

    def process_message(self, pkt, message, category):
        self.cache.append(message)
        if len(self.cache) > 10:
            self.dispatch(pkt, cPickle.dumps(self.cache), category)
            self.cache = []

    def run(self, pkt, tweet):
        tweet = cPickle.loads(tweet)
        for t in tweet:
            m = re.search('W(\s+)(.*)(\n)', t, re.S)
            if m:
                tw = m.group(2)
                if self.check(tw):
                   self.process_message(pkt, t, "POSITIVE")
