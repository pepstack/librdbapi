redplus.exe -S"SELECT * FROM xsdb.connect OFFSET 0 LIMIT 20;"
redplus.exe -S"SELECT * FROM xsdb.logentry"

redplus.exe -S"CREATE TABLE IF NOT EXISTS xsdb.connect (sid UB4 NOT NULL COMMENT '����ID', connfd UB4 NOT NULL COMMENT '����������', sessionid UB8X COMMENT '��ʱ�Ự��', port UB4 COMMENT '�ͻ��˶˿�', host STR(30) COMMENT '�ͻ���IP��ַ', hwaddr STR(30) COMMENT '������ַ(�ַ���)', agent STR(30) COMMENT '�����ն˴�������', ROWKEY(sid | connfd) ) COMMENT '�ͻ�����ʱ���ӱ�';"