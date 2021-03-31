import time
from elasticsearch import Elasticsearch
from elasticsearch import helpers

es = Elasticsearch(['10.111.203.128:9204'])#连接主机

#print(es.search(index='dialogue_pair',filter_path=['hits.hits._source','hits.total']))

#print(es.search(index='dialogue_pair', body={"query": {"match":{"Question": '''hi , how are you doing ? i'm getting ready to do some cheetah chasing to stay in shape .'''}}}))

print(es.delete_by_query(index='dialogue_pair', body={"query": {"match_all":{}}}))
