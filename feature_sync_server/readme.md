1. get_sync_status
    GET
    http://192.168.6.157:8000/feature?command=get_sync_status&province_code=320000&city_code=321000

2. query_user_range
    GET
    http://192.168.6.157:8000/feature?command=query_user_range&province_code=320000&city_code=321000&begin_id=-1&end_id=20

3. add_user
    POST
    http://192.168.6.157:8000/feature
        {
            "command": "add_user",
            "city_code": "321000",
            "uid": "d451b14537a6429bb8bab9a2ea6ec27e",
            "id": 58,
            "province_code": "320000",
            "pic_md5": "",  # base64.b64encode(src_pic_md5).decode()
            "town_code": "321002"
        }

4. del_user_by_id
    POST
    http://192.168.6.157:8000/feature

5. update_user
    POST
    http://192.168.6.157:8000/feature

6. query_feature_model_0330_range
    GET
    http://192.168.6.157:8000/feature?command=query_feature_model_0330_range&province_code=320000&city_code=321000&begin_id=-1&end_id=200

7. add_feature_model_0330
    POST
    http://192.168.6.157:8000/feature
    {
        "command": "add_feature_model_0330",
        "town_code": "321084",
        "city_code": "321000",
        "province_code": "320000",
        "timestamp": 1517541181,
        "feature": "",  # src_feature.decode()
        "id": 3195,
        "feature_id": 0,
        "user_id": 32798
    }

8. del_feature_model_0330_by_id
    POST
    http://192.168.6.157:8000/feature


创建表：

CREATE TABLE `user` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `uid` varchar(254) NOT NULL,
  `pic_md5` blob,
  `province_code` varchar(64) DEFAULT NULL,
  `town_code` varchar(64) DEFAULT NULL,
  `city_code` varchar(64) DEFAULT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `uid` (`uid`),
  KEY `user_uid_idx` (`uid`)
) ENGINE=InnoDB AUTO_INCREMENT=148197 DEFAULT CHARSET=latin1;


CREATE TABLE `feature_model_0330` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `user_id` int(11) DEFAULT NULL,
  `timestamp` int(11) NOT NULL,
  `feature_id` int(11) NOT NULL,
  `feature` mediumblob NOT NULL,
  `province_code` varchar(64) DEFAULT NULL,
  `city_code` varchar(64) DEFAULT NULL,
  `town_code` varchar(64) DEFAULT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `user_id` (`user_id`),
  CONSTRAINT `fk_feature_0330_user` FOREIGN KEY (`user_id`) REFERENCES `user` (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=144143 DEFAULT CHARSET=latin1;
