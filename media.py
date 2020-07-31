import csv
import redis
import json
import os
from urllib import parse
import pymysql
db = pymysql.connect("127.0.01","comment","1111","comment_db_hk" )
cursor = db.cursor()
from rediscluster import RedisCluster
startup_nodes = [
    {"host":"127.0.0.1", "port":6379}
]
r = RedisCluster(startup_nodes=startup_nodes, decode_responses=True,skip_full_coverage_check=True)
i=1
with open('dump', 'rt', encoding='utf-8', newline='') as csvfile:
    reader = csv.DictReader(csvfile)
    for row in reader:
        puaId= row['puaId']
        avatar = row['avatar']
        if avatar == 'NULL':
           avatar = 'hxxxxxxx'
           avatars = 'hhxxxxxxx'
        else:
           avatar = row['avatar']
           avatars = row['avatar']
        isVerified = row['isVerified']
        if isVerified=='0':
           isVerified = False
        else:
           isVerified = True
        hk01_nickname = row['name']
        dict = {"hk01_nickname":hk01_nickname,"avatar":{"hk01_user_avatar_small":avatar},"isVerified":isVerified}
        hk01_account_info = json.dumps(dict)
       
        #filterPath = os.path.basename(path.path)
        #filterPath = os.path.splitext(filterPath)[0]
        #dict = {"media_id":media_id,"width":width,"height":height}
        #data = json.dumps(dict)

        try:
            comment_user_id = row['puaId']
            comment_user_type = 2
            comment_user_name = row['name']
            comment_user_nickname = row['name']
            comment_user_avatar = avatars
            create_at = 1582281990
            update_at = 1582281990
            isverified = row['isVerified']
            #stmt='insert into dw_comment_pua_members_relations (comment_user_id) values (%s)'
            cursor.execute("replace into dw_comment_pua_members_relations (comment_user_id,comment_user_type,comment_user_name,comment_user_nickname,comment_user_avatar,create_at,update_at,isverified) values (%s, %s,%s, %s,%s, %s,%s, %s)", [comment_user_id,comment_user_type,comment_user_name,comment_user_nickname,comment_user_avatar,create_at,update_at,isverified])
            #ds = (id,)
            #cursor.execute(stmt)
            #print(cursor.rowcount)
            #r.set(puaId,hk01_account_info)
            #cursor.execute("select * from dw_comment_pua_members_relations")
            #res = cursor.fetchall()
            db.commit()
        except:
            db.rollback()
        r.set(puaId,hk01_account_info)
        print(hk01_account_info)
        #print(res)
        #print(data)
        #print(filterPath)
        #r.set(media_id,width)
        #r.set(media_id,width)
        #print(r.get('media_id'))
        #media_id,name,caption,original_path,width,height,type,admin,create_date,image_options,article_id

#conn.close()
