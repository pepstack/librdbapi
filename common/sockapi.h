/***********************************************************************
* Copyright (c) 2018 pepstack, pepstack.com
*
* This software is provided 'as-is', without any express or implied
* warranty.  In no event will the authors be held liable for any damages
* arising from the use of this software.
* Permission is granted to anyone to use this software for any purpose,
* including commercial applications, and to alter it and redistribute it
* freely, subject to the following restrictions:
*
* 1. The origin of this software must not be misrepresented; you must not
*   claim that you wrote the original software. If you use this software
*   in a product, an acknowledgment in the product documentation would be
*   appreciated but is not required.
*
* 2. Altered source versions must be plainly marked as such, and must not be
*   misrepresented as being the original software.
*
* 3. This notice may not be removed or altered from any source distribution.
***********************************************************************/

/**
 * sockapi.h
 *   for windows and linux
 *
 * create: 2014-08-01
 * update: 2019-04-02
 *
 */
#ifndef SOCKAPI_H_INCLUDED
#define SOCKAPI_H_INCLUDED

#if defined(__cplusplus)
extern "C" {
#endif

#if (defined(WIN32) || defined(_WIN16) || defined(_WIN32) || defined(_WIN64)) && !defined(__WINDOWS__)
# define __WINDOWS__
#endif

#include <string.h>
#include <stdio.h>
#include <stdlib.h>

#include <fcntl.h>
#include <errno.h>
#include <signal.h>

#include <sys/types.h>
#include <sys/stat.h>

#ifndef SOCKAPI_BUFSIZE
# define SOCKAPI_BUFSIZE      8192
#endif

/**
 * cross-platform sleep function
 */
#if defined(__WINDOWS__)
# define SOCKAPI_USES_SELECT

# include <windows.h>
# include <winsock2.h>
# include <ws2tcpip.h>

// Need to link with Ws2_32.lib
# pragma comment(lib, "ws2_32.lib")

# define sleep_msec(ms)  Sleep(ms)

  static void win32_strerror (int errcode, char *errmsg, int msglen)
    {
        FormatMessageA(FORMAT_MESSAGE_FROM_SYSTEM | FORMAT_MESSAGE_IGNORE_INSERTS,   // flags
                   NULL,                // lpsource
                   errcode,             // message id
                   MAKELANGID (LANG_ENGLISH, SUBLANG_ENGLISH_US),    // languageid
                   errmsg,              // output buffer
                   msglen,              // max length of error string in bytes
                   NULL);               // va_list of arguments

        if (! *errmsg) {
            snprintf(errmsg, msglen, "error(%d) with no message", errcode);
        }

        errmsg[msglen] = '\0';
        return errmsg;
    }

#elif _POSIX_C_SOURCE >= 199309L
# include <time.h>   // for nanosleep

  // sleep in milliseconds
  static void sleep_msec (int milliseconds)
    {
        struct timespec ts;
        ts.tv_sec = milliseconds / 1000;
        ts.tv_nsec = (milliseconds % 1000) * 1000000;
        nanosleep(&ts, 0);
    }


  // sleep in micro seconds, do not use usleep
  static void sleep_usec (int us)
    {
        /**
         * 1 sec = 1000 ms (millisec)
         * 1 ms = 1000 us (microsec)
         * 1 us = 1000 ns (nanosec)
         * 1 sec = 1000 000 000 ns (nanosec)
         */
        struct timespec ts;

        ts.tv_sec = us / 1000000;
        ts.tv_nsec = (us % 1000000) * 1000;

        nanosleep(&ts, 0);
    }

#else
# include <unistd.h>  // usleep
# define sleep_msec(ms)  usleep((ms) * 1000)
# define sleep_usec(us)  usleep(us)
#endif

#if !defined(__WINDOWS__)
# include <sys/socket.h>
# include <sys/resource.h>
# include <sys/sendfile.h>
# include <sys/ioctl.h>

# include <netinet/in.h>
# include <netinet/tcp.h>
# include <net/if.h>

# include <arpa/inet.h>
# include <netdb.h>
# include <semaphore.h>
# include <stdarg.h>

# include <poll.h>
# include <sys/select.h>

# ifndef SOCKET
    typedef int SOCKET;
# endif

# define closesocket(sfd) close(sfd)

  static int pollinfd_timed (int fd, int timeo_ms)
    {
        int ret;
        struct pollfd pfd;

        pfd.fd = fd;
        pfd.events = POLLIN | POLLRDNORM;
        pfd.revents = 0;

        ret = poll(&pfd, 1, timeo_ms);

        /*  1: success
         *  0: timeout
         * -1: errno
         */
        return ret;
    }

  static int polloutfd_timed (int fd, int timeo_ms)
    {
        int ret;
        struct pollfd pfd;

        pfd.fd = fd;
        pfd.events = POLLOUT | POLLWRNORM;
        pfd.revents = 0;

        ret = poll(&pfd, 1, timeo_ms);

        /*  1: success
         *  0: timeout
         * -1: errno
         */
        return ret;
    }
#endif


/**
 * error codes
 */
#define SOCKAPI_BAD_SOCKET     ((SOCKET)(-1))

#define SOCKAPI_SUCCESS          0
#define SOCKAPI_ERROR          (-1)

#define SOCKAPI_ERROR_SOCKOPT  (-2)
#define SOCKAPI_ERROR_FDNOSET  (-3)
#define SOCKAPI_ERROR_TIMEOUT  (-4)
#define SOCKAPI_ERROR_SELECT   (-5)


/**
 * Watch fd to see when it has input.
 *
 * returns:
 *   -1 : error select
 *    0 : timeout with no ready
 *    1 : fd is ready for read now
 */
static int watch_fd_read (SOCKET fd, int timeoms)
{
    int rc;

#ifdef SOCKAPI_USES_SELECT
    /* Watch stdin (fd=0) to see when it has input. */
    fd_set rfds;
    struct timeval tv;

    FD_ZERO(&rfds);
    FD_SET(fd, &rfds);

    /* Wait up to time. */
    if (timeoms == -1) {
        tv.tv_sec = -1;
        tv.tv_usec = 0;
    } else {
        tv.tv_sec = timeoms / 1000;
        tv.tv_usec = (timeoms % 1000) * 1000;
    }

    rc = select((int) fd + 1, &rfds, NULL, NULL, &tv);
    /* Don't rely on the value of tv now! */
#else
    /* use pollfd than select */
    rc = pollinfd_timed(fd, timeoms);
#endif

    return rc;
}


static int watch_fd_write (SOCKET fd, int timeoms)
{
    int rc;

#ifdef SOCKAPI_USES_SELECT
    /* Watch stdin (fd=0) to see when it has input. */
    fd_set wfds;
    struct timeval tv;

    FD_ZERO(&wfds);
    FD_SET(fd, &wfds);

    /* Wait up to time. */
    if (timeoms == -1) {
        tv.tv_sec = -1;
        tv.tv_usec = 0;
    } else {
        tv.tv_sec = timeoms / 1000;
        tv.tv_usec = (timeoms % 1000) * 1000;
    }

    rc = select((int)fd + 1, NULL, &wfds, NULL, &tv);

    /* Don't rely on the value of tv now! */
#else
    /* use pollfd than select */
    rc = polloutfd_timed(fd, timeoms);
#endif

    return rc;
}


/**
 * To determine if the socket is connected, it is more usual to use getsockopt()
 *   rather than getpeername()
 */
static int socket_is_connected (SOCKET sock)
{
    int soerr;
    socklen_t len = sizeof(soerr);

    if (getsockopt(sock, SOL_SOCKET, SO_ERROR, (char *)&soerr, &len) == 0 && soerr == 0) {
        /* socket is connected */
        return 1;
    } else {
        return 0;
    }
}


/**
 * For Linux Client, there are 4 parameters to keep alive for TCP:
 *   tcp_keepalive_probes - detecting times
 *   tcp_keepalive_time - send detecting pkg after last pkg
 *   tcp_keepalive_intvl - interval between detecting pkgs
 *   tcp_retries2 -
 * On Linux OS, use "echo" to update these params:
 *   echo "30" > /proc/sys/net/ipv4/tcp_keepalive_time
 *   echo "6" > /proc/sys/net/ipv4/tcp_keepalive_intvl
 *   echo "3" > /proc/sys/net/ipv4/tcp_keepalive_probes
 *   echo "3" > /proc/sys/net/ipv4/tcp_retries2
 * tcp_keepalive_time and tcp_keepalive_intvl is by seconds
 *   if keep them after system reboot, must add to: /etc/sysctl.conf file
 */
static int setsockalive (SOCKET sd, int keepIdle, int keepInterval, int keepCount)
{
    int keepAlive = 1;

    socklen_t len = (socklen_t) sizeof(keepAlive);

    /* enable keep alive */
    setsockopt(sd, SOL_SOCKET, SO_KEEPALIVE, (void *)&keepAlive, sizeof(keepAlive));

#if !defined(__WINDOWS__)
    /* tcp idle time, test time internal, test times */
    if (keepIdle > 0) {
        setsockopt(sd, SOL_TCP, TCP_KEEPIDLE, (void*)&keepIdle, sizeof(keepIdle));
    }
    if (keepInterval > 0) {
        setsockopt(sd, SOL_TCP, TCP_KEEPINTVL, (void *)&keepInterval, sizeof(keepInterval));
    }
    if (keepCount > 0) {
        setsockopt(sd, SOL_TCP, TCP_KEEPCNT, (void *)&keepCount, sizeof(keepCount));
    }
#endif

    /* Check the status again */
    if (getsockopt(sd, SOL_SOCKET, SO_KEEPALIVE, (char *)&keepAlive, &len) < 0) {
        return SOCKAPI_ERROR;
    }

    return keepAlive;
}


static int setsocktimeo (SOCKET sd, int sendTimeoutSec, int recvTimeoutSec)
{
    if (sendTimeoutSec > 0) {
        struct timeval sndtime = {sendTimeoutSec, 0};

        if (SOCKAPI_SUCCESS != setsockopt(sd, SOL_SOCKET, SO_SNDTIMEO, (char*) &sndtime, sizeof(struct timeval))) {
            return SOCKAPI_ERROR;
        }
    }

    if (recvTimeoutSec > 0) {
        struct timeval rcvtime = {recvTimeoutSec, 0};

        if (SOCKAPI_SUCCESS != setsockopt(sd, SOL_SOCKET, SO_RCVTIMEO, (char*) &rcvtime, sizeof(struct timeval))) {
            return SOCKAPI_ERROR;
        }
    }

    return SOCKAPI_SUCCESS;
}


static int setsocketblock (SOCKET sfd, char *errmsg)
{
#if defined(__WINDOWS__)
    unsigned long flags = 0;
    int err = ioctlsocket(sfd, FIONBIO, (unsigned long *)&flags);
    if (err == SOCKET_ERROR && errmsg) {
        win32_strerror(WSAGetLastError(), errmsg, 127);
    }
    return err;
#else
    int flags = fcntl(sfd, F_GETFL, 0);
    if (flags == -1) {
        if (errmsg) {
            snprintf(errmsg, 127, "fcntl(F_GETFL) error(%d): %s", errno, strerror(errno));
            errmsg[127] = '\0';
        }
        return -1;
    }

    if (fcntl(sfd, F_SETFL, flags & ~O_NONBLOCK) == -1) {
        if (errmsg) {
            snprintf(errmsg, 127, "fcntl(F_SETFL) error(%d): %s", errno, strerror(errno));
            errmsg[127] = '\0';
        }
        return -1;
    }

    /* success */
    return 0;
#endif
}


static int setsocketnonblock (SOCKET sfd, char *errmsg)
{
#if defined(__WINDOWS__)
    unsigned long flags = 1;
    int err = ioctlsocket(sfd, FIONBIO, (unsigned long *)&flags);
    if (err == SOCKET_ERROR && errmsg) {
        win32_strerror(WSAGetLastError(), errmsg, 127);
    }
    return err;
#else
    int flags = fcntl(sfd, F_GETFL, 0);
    if (flags == -1) {
        if (errmsg) {
            snprintf(errmsg, 127, "fcntl(F_GETFL) error(%d): %s", errno, strerror(errno));
            errmsg[127] = '\0';
        }
        return -1;
    }

    if (fcntl(sfd, F_SETFL, flags | O_NONBLOCK) == -1) {
        if (errmsg) {
            snprintf(errmsg, 127, "fcntl(F_SETFL) error(%d): %s", errno, strerror(errno));
            errmsg[127] = '\0';
        }
        return -1;
    }

    /* success */
    return 0;
#endif
}


static int connect_nb_timed (SOCKET sfd, const struct sockaddr *addr, int addrlen, int timeout_ms, char *errmsg)
{
    int err;
    int len = sizeof(err);

    unsigned long nonblock = 1;

    if (setsocketnonblock(sfd, errmsg) != 0) {
        return (-1);
    }

    err = connect(sfd, addr, addrlen);
    if (err == 0) {
        return 0;
    } else {
        struct timeval tm;
        fd_set set;

        tm.tv_sec  = timeout_ms / 1000;
        tm.tv_usec = (timeout_ms % 1000) * 1000;

        FD_ZERO(&set);
        FD_SET(sfd, &set);

        err = select((int) sfd + 1, 0, &set, 0, &tm);
        if (err > 0) {
            if (FD_ISSET(sfd, &set)) {
                // must add below for firewall
                if (getsockopt(sfd, SOL_SOCKET, SO_ERROR, (char *) &err, &len) != 0) {
                    err = -1;
                    if (errmsg) {
                        snprintf(errmsg, 127, "getsockopt error(%d): %s", errno, strerror(errno));
                    }
                    goto onerror_ret;
                }

                if (err == 0) {
                    // success               
                    return 0;
                }
            }

            err = -2;

            if (errmsg) {
                snprintf(errmsg, 127, "select error: FD_ISSET");
            }
            goto onerror_ret;
        } else if (err == 0) {
            // timeout
            err = -3;

            if (errmsg) {
                snprintf(errmsg, 127, "select timeout: more than %d ms", timeout_ms);
            }
            goto onerror_ret;
        } else {
            // error
            err = -4;

            if (errmsg) {
                snprintf(errmsg, 127, "select error(%d): %s", errno, strerror(errno));
            }
            goto onerror_ret;
        }
    }

onerror_ret:
    return err;
}


static SOCKET opensocketnonblock (const char *host, const char *port, int timeout_ms, char errmsg[128])
{
    struct addrinfo hints;
    struct addrinfo *res, *rp;

    int err;

    SOCKET sockfd = SOCKAPI_BAD_SOCKET;

    errmsg[0] = errmsg[127] = 0;

    /* Obtain address(es) matching host/port */
    memset(&hints, 0, sizeof(struct addrinfo));

    hints.ai_family = AF_UNSPEC;      /* Allow IPv4 or IPv6 */
    hints.ai_socktype = SOCK_STREAM;  /* We want a TCP socket */
    hints.ai_flags = 0;
    hints.ai_protocol = 0;            /* Any protocol */

    err = getaddrinfo(host, port, &hints, &res);
    if (err != 0) {
        snprintf(errmsg, 127, "getaddrinfo error(%d): %s", err, gai_strerror(err));
        return SOCKAPI_BAD_SOCKET;
    }

    /**
     * getaddrinfo() returns a list of address structures.
     *
     * Try each address until we successfully connect(2).
     * If socket(2) (or connect(2)) fails, we (close the socket and)
     *   try the next address.
     */
    for (rp = res; rp != NULL; rp = rp->ai_next) {
        /**
         * socket()
         *
         *   create an endpoint for communication
         *     https://linux.die.net/man/2/socket
         *   On success, a file descriptor for the new socket is returned.
         *   On error, -1 is returned, and errno is set appropriately.
         */
        sockfd = socket(rp->ai_family, rp->ai_socktype, rp->ai_protocol);
        if (sockfd == -1) {
            snprintf(errmsg, 127, "socket error(%d): %s", errno, strerror(errno));
            continue;
        }

        err = connect_nb_timed(sockfd, rp->ai_addr, (int) rp->ai_addrlen, timeout_ms, errmsg);
        if (err == 0) {
            /* connect success */
            freeaddrinfo(res);
            return sockfd;
        }

        closesocket(sockfd);
    }

    if (! errmsg[0]) {
        snprintf(errmsg, 127, "bad address(%.*s:%s)", (int)strnlen(host, 96), host, port);
    }

    freeaddrinfo(res);
    return SOCKAPI_BAD_SOCKET;
}


static int setsocketbuflen (SOCKET s, int rcvlen, int sndlen)
{
    socklen_t rcv, snd;
    socklen_t rcvl = (socklen_t) sizeof(int);
    socklen_t sndl = rcvl;

    /* default size is 8192 */
    if (getsockopt(s, SOL_SOCKET, SO_RCVBUF, (char*)&rcv, &rcvl) == SOCKAPI_BAD_SOCKET ||
        getsockopt(s, SOL_SOCKET, SO_SNDBUF, (char*)&snd, &sndl) == SOCKAPI_BAD_SOCKET) {
        return SOCKAPI_ERROR;
    }

    if (rcv < rcvlen) {
        rcv = rcvlen;
        rcvl = setsockopt(s, SOL_SOCKET, SO_RCVBUF, (char*) &rcv, rcvl);
    }

    if (snd < sndlen){
        snd = sndlen;
        sndl = setsockopt(s, SOL_SOCKET, SO_SNDBUF, (char*)&snd, sndl);
    }

    return SOCKAPI_SUCCESS;
}


static int recvlen (SOCKET fd, char * buf, int len)
{
    int    rc;
    char * pb = buf;

    while (len > 0) {
        rc = recv (fd, pb, len, 0);
        if (rc > 0) {
            pb += rc;
            len -= rc;
        } else if (rc < 0) {
            if (errno != EINTR) {
                /* error returned */
                return -1;
            }
        } else {
            /* rc == 0: socket has closed */
            return 0;
        }
    }

    return (int)(pb - buf);
}


static int sendlen (SOCKET fd, const char* msg, int len)
{
    int    rc;
    const char * pb = msg;

    while (len > 0) {
        rc = send(fd, pb, len, 0);
        if (rc > 0) {
            pb += rc;
            len -= rc;
        } else if (rc < 0) {
            if (errno != EINTR) {
                /* error returned */
                return -1;
            }
        } else {
            /* socket closed */
            return 0;
        }
    }

    return (int)(pb - msg);
}


static int send_bulky (SOCKET s, const char* bulk, int bulksize, int sndl /* length per msg to send */)
{
    int snd, ret;
    int left = bulksize;
    int at = 0;

    while (left > 0) {
        snd = left > sndl? sndl : left;
        ret = sendlen(s, &bulk[at], snd);
        if (ret != snd) {
            return ret;
        }
        at += ret;
        left -= ret;
    }

    return at;
}


static int recv_bulky (SOCKET s, char* bulk, int len, int rcvl/* length per msg to recv */)
{
    int  rcv, ret;
    int  left = len;
    int  at = 0;

    while (left > 0) {
        rcv = left > rcvl? rcvl : left;
        ret = recvlen(s, &bulk[at], rcv);
        if (ret != rcv) {
            return ret;
        }
        at += ret;
        left -= ret;
    }

    return at;
}


#if !defined(__WINDOWS__)
static ssize_t pread_len (SOCKET fd, unsigned char *buf, size_t len, off_t pos)
{
    ssize_t rc;
    unsigned char *pb = buf;

    while (len != 0 && (rc = pread (fd, (void*) pb, len, pos)) != 0) {
        if (rc == -1) {
            if (errno == EINTR || errno == EAGAIN) {
                continue;
            } else {
                /* pread error */
                return SOCKAPI_ERROR;
            }
        }

        len -= rc;
        pos += rc;
        pb += rc;
    }

    return (ssize_t) (pb - buf);
}


static ssize_t sendfile_len (SOCKET sockfd, int filefd, size_t len, off_t pos)
{
    ssize_t rc;

    off_t  off = pos;

    while (len != 0 && (rc = sendfile (sockfd, filefd, &off, len)) != 0) {
        if (rc == -1) {
            if (errno == EINTR || errno == EAGAIN) {
                continue;
            } else {
                return -1;
            }
        }

        /* rc > 0 */
        len -= rc;
    }

    return (ssize_t) (off - pos);
}
#endif


/**********************************************************************
 *                                                                    *
 *                        socket nonblock api                         *
 *                                                                    *
 **********************************************************************/

/**
 * writemsg_nb
 *   write in nonblock mode
 * returns:
 *   1: success with written size saved in offset.
 *   0: connect closed. no data available.
 *  -1: no data in buffer with actual size of data written saved in offset.
 *   2: wait fd for writable timeout if wait_ms set
 *  -2: wait fd for writable error if wait_ms set
 */
static int writemsg_nb (SOCKET fd, const char* msgbuf, size_t msglen, off_t *offset, int wait_ms)
{
    int count;

    off_t offlen = 0;

    *offset = 0;

    if (wait_ms) {
        int err = watch_fd_write(fd, wait_ms);

        if (err == 0) {
            printf("(sockapi.h:%d writemsg_nb) watch_fd_write timeout\n", __LINE__);
            return 2;
        }

        if (err == -1) {
            printf("(sockapi.h:%d writemsg_nb) watch_fd_write error(%d): %s\n", __LINE__, errno, strerror(errno));
            return -2;
        }
    }

    for (;;) {
        count = (int)
            #if defined(__WINDOWS__)
                send(fd, msgbuf + offlen, (int) (msglen - offlen), 0);
            #else
                write(fd, msgbuf + offlen, msglen - offlen);
            #endif

        if (count == -1) {
            if (errno == EPIPE) {
                /* fd is connected to a pipe or socket whose reading end is
                 * closed.  When this happens the writing process will also
                 * receive a SIGPIPE signal.  (Thus, the write return value is
                 * seen only if the program catches, blocks or ignores this
                 * signal.)
                 */
            #if defined(__WINDOWS__)
                printf("(sockapi.h:%d writemsg_nb) send error(%d): %s\n", __LINE__, errno, strerror(errno));
            #else
                printf("(sockapi.h:%d writemsg_nb) write error(%d): %s\n", __LINE__, errno, strerror(errno));
            #endif
                return 0;
            }

            if (errno != EAGAIN && errno != ECONNABORTED && errno != EPROTO && errno != EINTR) {
            #if defined(__WINDOWS__)
                printf("(sockapi.h:%d writemsg_nb) send error(%d): %s\n", __LINE__, errno, strerror(errno));
            #else
                printf("(sockapi.h:%d writemsg_nb) write error(%d): %s\n", __LINE__, errno, strerror(errno));
            #endif
                continue;
            }

            // EAGAIN: Resource temporarily unavailable
            // in nonblock mode when errno = EAGAIN indicates there is no data in buffer
            // for r/w

            // set bytes has been written
            *offset = offlen;

            // -1: errno == EAGAIN (there is no data in buffer)
            return (-1);
        }

        if (count > 0) {
            offlen += (off_t) count;

            if (offlen == msglen) {
                // msg data write success
                *offset = offlen;
                return 1;
            }
        } else {
            // The remote has closed the connection
            return 0;
        }
    }
}


/**
 * readmsg_nb
 *   read in nonblock mode
 * returns:
 *   1: success with read size saved in offset.
 *   0: connect has been closed. no data available.
 *  -1: no data in buffer with actual size of data read saved in offset.
 *   2: wait fd for readable timeout if wait_ms set
 *  -2: wait fd for readable error if wait_ms set
 */
static int readmsg_nb (SOCKET fd, char *msgbuf, size_t msglen, off_t *offset, int wait_ms)
{
    int count;

    off_t offlen = 0;

    *offset = 0;

    if (wait_ms) {
        int err;

        // use the poll system call to be notified about socket status changes
        err = watch_fd_read(fd, wait_ms);
        if (err == 0) {
            printf("(sockapi.h:%d readmsg_nb) watch_fd_read timeout\n", __LINE__);
            return 2;
        }

        if (err == -1) {
            printf("(sockapi.h:%d readmsg_nb) watch_fd_read error(%d): %s\n", __LINE__, errno, strerror(errno));
            return -2;
        }
    }

    for(;;) {
        count = (int)
            #if defined(__WINDOWS__)
                recv(fd, msgbuf + offlen, (int)(msglen - offlen), 0);
            #else
                read(fd, msgbuf + offlen, msglen - offlen);
            #endif

        if (count == -1) {
            if (errno == EPIPE) {
                // The remote has closed the connection
            #if defined(__WINDOWS__)
                printf("(sockapi.h:%d readmsg_nb) recv error(%d): %s\n", __LINE__, errno, strerror(errno));
            #else
                printf("(sockapi.h:%d readmsg_nb) read error(%d): %s\n", __LINE__, errno, strerror(errno));
            #endif
                return 0;
            }

            if (errno != EAGAIN && errno != EWOULDBLOCK && errno != ECONNABORTED && errno != EPROTO && errno != EINTR) {
        #if defined(__WINDOWS__)
            printf("(sockapi.h:%d readmsg_nb) recv error(%d): %s\n", __LINE__, errno, strerror(errno));
        #else
            printf("(sockapi.h:%d readmsg_nb) read error(%d): %s\n", __LINE__, errno, strerror(errno));
        #endif
                continue;
            }

            // EAGAIN: Resource temporarily unavailable
            // in nonblock mode when errno = EAGAIN indicates there is no data in buffer
            // for r/w

            // set size in bytes of read
            *offset = offlen;

            // -1: errno == EAGAIN (there is no data to read in buffer)
            return (-1);
        }

        if (count > 0) {
            offlen += (off_t) count;
            if (offlen == msglen) {
                // msg data read success
                *offset = offlen;
                return 1;
            }
        } else {
            // The remote has closed the connection
            return 0;
        }
    }
}


#if defined(__cplusplus)
}
#endif

#endif /* SOCKAPI_H_INCLUDED */
