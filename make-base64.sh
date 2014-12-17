#!/usr/bin/bash

cat language_filter.py | base64 | tr -d '\n' > language_filter.py.b64
cat named_entity_filter.py | base64 | tr -d '\n' > named_entity_filter.py.b64
cat topic_filter.py | base64 | tr -d '\n' > topic_filter.py.b64
cat sentiment_analyses.py | base64 | tr -d '\n' > sentiment_analyses.py.b64
cat collectorall.py | base64 | tr -d '\n' > collectorall.py.b64
apack pmk-seeds.tar collectorall.py language_filter.py named_entity_filter.py sentiment_analyses.py topic_filter.py tweetinject.py
