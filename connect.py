#coding=utf-8
import time
from elasticsearch import Elasticsearch
from elasticsearch import helpers

es = Elasticsearch(['10.111.203.128:9204'])#连接主机

#es.index(index='dialogue_pair', body={"Question": "麻子多大了", "answer": 21}) #插入一条文档示例

#print(es.delete_by_query(index='dialogue_pair', body={"query": {"match_all":{}}})) #删除全部文档

#print(es.search(index='dialogue_pair', body={"query": {"match":{"Querstion": 'hi , how are you doing ? i'm getting ready to do some cheetah chasing to stay in shape .'}}})) #检测所有文档

#print(es.search(index='dialogue_pair',filter_path=['hits.hits._source','hits.total'])) #查询已有的文档


def timer(func):
    def wrapper(*args, **kwargs):
        start = time.time()
        res = func(*args, **kwargs)
        print('共耗时约 {:.2f} 秒'.format(time.time() - start))
        return res
    return wrapper

@timer
def create_data():
    #写入单条数据
    f = open('D:/伴鱼/elasticsearch/test.txt',encoding='utf-8')
    #f = open('/data/xiayuancheng/es/alldialogs.txt', encoding='utf-8')
    for line in f:
        line = line.split('\t')
        es.index(index='dialogue_pair', body={"Question": line[0], 'answer': line[1]}) #单条的形式

@timer
def batch_data():
    #批量写入数据
    f = open('D:/伴鱼/elasticsearch/alldialogs.txt',encoding='utf-8')
    #f = open('/data/xiayuancheng/es/alldialogs.txt', encoding='utf-8')
    for line in f:
        line = line.split('\t')
        action = [{
            "_index": "dialogue_pair",
            "_type": "_doc",#注意这里是_doc而不是doc
            "_source": {
                "Question": line[0],
                "answer": line[1]
            }
        }]
        helpers.bulk(es, action)

if __name__ == '__main__':
    batch_data()
