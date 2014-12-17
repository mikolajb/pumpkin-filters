###START-CONF
##{
##"object_name": "tweetinject",
##"object_poi": "qpwo-2345",
##"auto-load": true,
##"remoting" : false,
##"parameters": [
##
##              ],
##"return": [
##              {
##                      "name": "tweet",
##                      "description": "raw tweet",
##                      "required": true,
##                      "type": "TweetString",
##                      "format": "",
##                      "state" : "RAW"
##                  }
##
##          ] }
##END-CONF



from os import listdir
from os.path import isfile, join
import pika, urllib2, cPickle

from os.path import expanduser
from pumpkin import *



class tweetinject(PmkSeed.Seed):

    def __init__(self, context, poi=None):
        PmkSeed.Seed.__init__(self, context,poi)
        self.connection = None
        self.channel = None
        self.cache = []

    def get_net_file(self, url, file_name):
        #file_name = url.split('/')[-1]
        downloaded = False
        while not downloaded:
            # try:
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

    def on_load(self):
        print "Loading: " + self.__class__.__name__
        ok = False
        url = "http://elab.lab.uvalight.net/gzs/tweets2009-06-brg.txt"
        dir = expanduser("~")+"/tweets/"
        output_file = dir+"tweets2009-06-brg.txt"
        self._ensure_dir(dir)
        onlyfiles = [ f for f in listdir(dir) if isfile(join(dir,f)) ]

        for fl in onlyfiles:
            fullpath = dir+fl
            if( fl[-3:] == "txt"):
                ok = True
                break

        if not ok:
            self.get_net_file(url,output_file)

    def process_message(self, pkt, message, category):
        self.cache.append(message)
        if len(self.cache) > 10:
            self.fork_dispatch(pkt, cPickle.dumps(self.cache), category)
            self.cache = []

    def run(self, pkt):
        dir = expanduser("~")+"/tweets/"
        onlyfiles = [ f for f in listdir(dir) if isfile(join(dir,f)) ]
        for fl in onlyfiles:
            fullpath = dir+fl
            if( fl[-3:] == "txt"):
                print "File: "+str(fl)
                with open(fullpath) as f:
                    for line in f:
                        if line.startswith('T'):
                            tweet = line
                        if line.startswith("U"):
                            tweet = tweet + line
                        if line.startswith("W"):
                            if line == "No Post Title":
                                line =""
                            else:
                                tweet = tweet + line
                                self.process_message(pkt, tweet, "RAW")
                                del line
                                del tweet
