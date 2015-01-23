__author__ = 'reggie'


###START-CONF
##{
##"object_name": "collectorall",
##"object_poi": "qpwo-2345",
##"parameters": [
##                 {
##                      "name": "tweet",
##                      "description": "english haiku tweets",
##                      "required": true,
##                      "type": "TweetString",
##                      "format": "",
##                      "state" : "ENTITIES"
##                  }
##              ],
##"return": [
##
##          ] }
##END-CONF



import re, cPickle
from pumpkin import PmkSeed
import json
import re
import networkx as nx
from networkx.readwrite import json_graph
from time import gmtime, strftime

class collectorall(PmkSeed.Seed):

    def __init__(self, context, poi=None):
        PmkSeed.Seed.__init__(self, context,poi)

        pass

    def on_load(self):
        self.logger.info("Loading: " + self.__class__.__name__)
        self.stats = open("stats.txt", "w+")

    def run(self, pkt, tweet):
        tweet = cPickle.loads(tweet)
        self.stats.write(strftime("%a, %d %b %Y %H:%M:%S +0000\n", gmtime()))
        self.stats.flush()
        for t in tweet:
            self.logger.info("collector: " + t)
