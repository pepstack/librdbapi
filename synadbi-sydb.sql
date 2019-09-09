# redisdb table: {sydb::neuronid:$neuronid}
DROP TABLE sydb.neuronid;
CREATE TABLE IF NOT EXISTS sydb.neuronid (
    status               UB4                   COMMENT '客户端状态标识',
    maxspeed             UB4                   COMMENT '该客户的最大传输速度限制<KBS. =0 不限制>',
    uid                  UB8                   COMMENT '客户端的唯一数字ID',
    neuronid             STR(40)      NOT NULL COMMENT '客户端指定的唯一名称<字符串>. 用于登录验证',
    maxagents            UB4                   COMMENT '客户端同时连接的终端数限制<=0 不限制>',
    secret               STR(60)               COMMENT '客户密码. 验证客户登录',
    version              UB4                   COMMENT '客户端版本号',
    cretime              UB8                   COMMENT '客户端创建时间戳<秒>',
    maxsessions          UB4                   COMMENT '客户的最大会话数限制<=0 不限制>',
    endtime              UB8                   COMMENT '客户端终止时间戳<秒>',
    ROWKEY (neuronid)
) COMMENT '客户端注册表';

# redisdb table: {sydb::neuron:$uid}
DROP TABLE sydb.neuron;
CREATE TABLE IF NOT EXISTS sydb.neuron (
    uid                  UB8          NOT NULL COMMENT '客户端ID',
    neuronid             STR(40)               COMMENT '客户端唯一名称<字符串>',
    sessions             UB4                   COMMENT '客户当前的连接会话数',
    ROWKEY (uid)
) COMMENT '客户信息表';

# redisdb table: {sydb::sessionid:$sessionid}
DROP TABLE sydb.sessionid;
CREATE TABLE IF NOT EXISTS sydb.sessionid (
    status               UB4                   COMMENT '会话状态标识',
    uid                  UB8                   COMMENT '客户端ID',
    randnum              UB4                   COMMENT '随机数',
    sessiontoken         UB8X                  COMMENT '会话的令牌<16进制>',
    sessionid            UB8X         NOT NULL COMMENT '连接会话键值<16进制>',
    parentsessionid      UB8X                  COMMENT '父会话<16进制>',
    sid                  UB4                   COMMENT '服务ID',
    entryid              UB8X                  COMMENT '当前会话正在处理的文件条目<16进制>',
    connfd               UB4                   COMMENT '会话的 socket 描述符',
    ROWKEY (sessionid)
) COMMENT '临时会话表. 键值在会话过期后自动删除';

# redisdb table: {sydb::session:$uid:$sessionid}
DROP TABLE sydb.session;
CREATE TABLE IF NOT EXISTS sydb.session (
    expdt                UB8                   COMMENT '会话过期时间',
    sessionid            UB8X         NOT NULL COMMENT '会话ID',
    parentsessionid      UB8X                  COMMENT '主连接会话ID',
    uid                  UB8          NOT NULL COMMENT '客户端ID',
    randnum              UB4                   COMMENT '随机数',
    credt                UB8                   COMMENT '会话创建时间',
    ROWKEY (uid , sessionid)
) COMMENT '客户端会话状态表';

# redisdb table: {sydb::entryid:$entryid}
DROP TABLE sydb.entryid;
CREATE TABLE IF NOT EXISTS sydb.entryid (
    status               UB4                   COMMENT '状态标记',
    entrytoken           UB8X                  COMMENT '访问条目的验证令牌<16进制>',
    updtime              UB8                   COMMENT '服务最后保存文件的时间',
    modtime              UB8                   COMMENT '客户端最后文件变化的时间',
    mask                 UB4                   COMMENT '文件事件掩码',
    sessionid            UB8X                  COMMENT '连接会话键值<16进制>',
    entryid              UB8X         NOT NULL COMMENT '为文件传输而动态分配的条目ID<16进制>',
    filemode             UB4                   COMMENT '条目文件的权限模式',
    entrykey             STR(32)               COMMENT '文件条目全路径字符串的md5值',
    offsetcb             UB8                   COMMENT '服务最后保存文件的字节偏移',
    entryfile            STR(895)              COMMENT '文件条目的服务端全路径名<=$logstash+$logfile>',
    ROWKEY (entryid)
) COMMENT '同步日志文件状态临时表';

# redisdb table: {sydb::cortex:$sid}
DROP TABLE sydb.cortex;
CREATE TABLE IF NOT EXISTS sydb.cortex (
    status               UB4                   COMMENT '服务的状态标识',
    connfds              UB4                   COMMENT '当前总的 socket 连接数',
    queues               UB4                   COMMENT '服务队列大小',
    host                 STR(127)              COMMENT '服务的主机名称<ip地址>',
    neurons              UB4                   COMMENT '当前连接的客户端数',
    threads              UB4                   COMMENT '服务线程总数',
    sid                  UB4          NOT NULL COMMENT '服务端唯一数字ID',
    cortexid             STR(40)               COMMENT '指定的服务名<字符串>',
    port                 UB4                   COMMENT '服务端口号',
    ROWKEY (sid)
) COMMENT '服务状态信息表';

# redisdb table: {sydb::logentry:$sid:$uid:$entrykey}
DROP TABLE sydb.logentry;
CREATE TABLE IF NOT EXISTS sydb.logentry (
    status               UB4                   COMMENT '文件的状态标记',
    updtime              UB8                   COMMENT '服务端文件最后更新的时间戳',
    uid                  UB8          NOT NULL COMMENT '客户端ID',
    addtime              UB4                   COMMENT '累计的时间<秒>',
    filetime             UB8                   COMMENT '客户端文件最后的时间戳',
    pathid               STR(96)               COMMENT '客户端的 pathid 名称',
    speedhigh            UB4                   COMMENT '最大传输速度<KBS>',
    logmd5               STR(32)               COMMENT '服务端文件最后的md5值<可选>',
    entrykey             STR(32)      NOT NULL COMMENT '文件条目的服务端全路径名的md5值 = md5sum<$logstash+$logfile>',
    logfile              STR(127)              COMMENT '客户端的日志文件名<不包括路径>',
    logstash             STR(768)              COMMENT '服务端保存的文件的绝对路径 <不包括文件名>',
    route                STR(512)              COMMENT '客户端的文件的路由',
    filemd5              STR(32)               COMMENT '客户端文件最后的md5值<可选>',
    speedlow             UB4                   COMMENT '最低传输速度<KBS>',
    entryid              UB8X                  COMMENT '为文件传输而动态分配的条目ID<16进制>',
    filesize             UB8                   COMMENT '客户端文件最后的字节数',
    sid                  UB4          NOT NULL COMMENT '服务端ID',
    cretime              UB8                   COMMENT '条目创建的时间戳',
    position             UB8                   COMMENT '服务端文件最后更新的字节位置<大小>',
    ROWKEY (sid , uid , entrykey)
) COMMENT '日志文件注册表. 永久保留此表';

# redisdb table: {sydb::cortexid:$cortexid}
DROP TABLE sydb.cortexid;
CREATE TABLE IF NOT EXISTS sydb.cortexid (
    cortexid             STR(40)      NOT NULL COMMENT '指定的服务名<字符串>',
    magic                UB8                   COMMENT '服务加密字',
    timeout              UB4                   COMMENT '用户登录会话超时秒',
    sid                  UB4                   COMMENT '服务唯一数字编号',
    maxneurons           UB4                   COMMENT '允许登录的最多客户端数',
    stashprefix          STR(127)              COMMENT '服务端文件存储的路径前缀',
    ROWKEY (cortexid)
) COMMENT '服务注册表';

# redisdb table: {sydb::connect:$sid:$connfd}
DROP TABLE sydb.connect;
CREATE TABLE IF NOT EXISTS sydb.connect (
    host                 STR(127)              COMMENT '客户端ip地址',
    agent                STR(255)              COMMENT '连接终端代理类型',
    sessionid            UB8X                  COMMENT '临时会话键',
    sid                  UB4          NOT NULL COMMENT '服务ID',
    hwaddr               STR(30)               COMMENT '网卡地址<字符串>',
    credt                STR(30)               COMMENT '创建时间<字符串>',
    connfd               UB4          NOT NULL COMMENT 'socket 连接描述符',
    port                 UB4                   COMMENT '客户端端口',
    ROWKEY (sid , connfd)
) COMMENT '客户端临时连接表. 键值在会话过期后自动删除';

