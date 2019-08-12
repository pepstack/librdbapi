# hiredis-w32

A Windows sample project uses hiredis lib for windows x86_64.

Test on Windows7 x64 with vs2013.


## Step-1: Get redis with hiredis for windows at:


- https://github.com/microsoftarchive/redis

and rename it to redis-win


Open redis-win/msvs/RedisServer.sln with ms vs2013 and build only the hiredis and Win32_Interop projects.


## Step-2: Get hiredis-w32 at:

- https://github.com/pepstack/hiredis-w32

and place it with redis-win side by side like below:


```
  your_path/
    |
    +---- redis-win/
    |
    +---- hiredis-w32/
    |
    ....
```

Once after you have built hiredis and Win32_Interop projects (see: Step-1), then double click update.bat in hiredis-w32/.

Now you get all sources and libs for hiredis-w32 up to date.

Open hiredis-w32/msvc/hiredis-w32-client.sln with ms vs2013 and build it as you need.

## Refer:

[windows hiredis compile](https://blog.csdn.net/xumaojun/article/details/51558128)


## Enable redis-server to accept remote connection

Make sure firewall is opened for PORT:


edit config file redis-PORT.conf for redis-server as below:

```
    ...

    ## only access for localhost
    ##bind                         127.0.0.1
    ##protected-mode               yes

    ## can access for remote
    #bind                         127.0.0.1
    protected-mode               no
    ...
```

Stop and start redis-server!

    redis-server /path/to/redis-PORT.conf


    


