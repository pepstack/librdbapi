redplus.exe -S"SELECT * FROM xsdb.connect OFFSET 0 LIMIT 20;"
redplus.exe -S"SELECT * FROM xsdb.logentry"

redplus.exe -S"CREATE TABLE IF NOT EXISTS xsdb.connect (sid UB4 NOT NULL COMMENT '服务ID', connfd UB4 NOT NULL COMMENT '连接描述符', sessionid UB8X COMMENT '临时会话键', port UB4 COMMENT '客户端端口', host STR(30) COMMENT '客户端IP地址', hwaddr STR(30) COMMENT '网卡地址(字符串)', agent STR(30) COMMENT '连接终端代理类型', ROWKEY(sid | connfd) ) COMMENT '客户端临时连接表';"