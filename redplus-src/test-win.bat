redplus.exe -S"SELECT * FROM xsdb.connect OFFSET 0 LIMIT 20;"
redplus.exe -S"SELECT * FROM xsdb.logentry"

redplus.exe -S"CREATE TABLE IF NOT EXISTS xsdb.connect (sid UB4 NOT NULL COMMENT 'server id', connfd UB4 NOT NULL COMMENT 'connect socket fd', sessionid UB8X COMMENT 'session id', port UB4 COMMENT 'client port', host STR(30) COMMENT 'client host ip', hwaddr STR(30) COMMENT 'mac addr', agent STR(30) COMMENT 'client agent', ROWKEY(sid | connfd) ) COMMENT 'log file entry';"