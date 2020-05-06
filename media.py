import csv
import redis
import json
import os
from urllib import parse
from rediscluster import RedisCluster

startup_nodes = [
    {"host":"127.0.0.1", "port":7000},
    {"host":"127.0.0.1", "port":7001},
    {"host":"127.0.0.1", "port":7002}
]
r = RedisCluster(startup_nodes=startup_nodes, decode_responses=True)


with open('media_2015', 'rt', encoding='utf-8', newline='') as csvfile:
    reader = csv.DictReader(csvfile)

    for row in reader:
        width = row['width']
        height = row['height']
        media_id= row['media_id']
        path  = parse.urlparse(row['url'])
        filterPath = os.path.basename(path.path)
        filterPath = os.path.splitext(filterPath)[0]
        r.set(filterPath,media_id)
        print(filterPath)



redis-cli -h prod-media-redis.bsrorv.clustercfg.apse1.cache.amazonaws.com -c -p 6379