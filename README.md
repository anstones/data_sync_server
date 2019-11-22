# data_sync_server

    适用于对端不暴露数据库只开放一个端口的情况
    服务端放在对端内网，用于接收特征
    客户端放在线上，查询特征推送至服务器。


#### python版本   

​	python  >=  3.6

#### 依赖

​	requirements.txt

​	pip3 install -r requirements.txt

### quick start

​	python3 feature_sync_client.py  [-c conf]
​	python3 feature_sync_server.py [-c conf]



