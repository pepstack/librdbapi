import os

dllpath = ".\\"

if dllpath not in os.sys.path:
    os.sys.path.append(dllpath)

import redisdbi

rdb = redisdbi.connect("test@192.168.39.111:7001-7009", 0, 12000)
print dir(rdb)

#ret = redisdbi.execute(rdb, "INFO")

#print(ret)

