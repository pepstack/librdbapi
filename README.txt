
redplus is a command tool to access redis cluster.

for linux rhel6, 7

author: zhangliang

1) edit redplus.cfg as you need.

   Make sure add all cluster nodes in it !

2) start as interactive mode:

   # rlwrap ./redplus -I


supported SQL dialect:

    SELECT * | COUNT(*) | field1, field2,... FROM database.table <WHERE condition1 AND condition2 AND ...> <OFFSET m> <LIMIT n>;

    DROP TABLE database.table;

    DESC database.table;

    SHOW DATABASES;

    SHOW TABLES database;

    DELETE FROM database.table <WHERE condition1 AND condition2 AND ...> <OFFSET m> <LIMIT n>; 

    UPSERT INTO database.table (field1, field2, ...) VALUES (value1, value2, ...) <ON DUPLICATE KEY IGNORE | UPDATE col1=val2, col2=val2,...>;
    
    CREATE TABLE database.table (id UB8 NOT NULL COMMENT 'global id', name STR(30) NOT NULL, ..., fieldname, ROWKEY(id,name)) <COMMENT '...'>;
