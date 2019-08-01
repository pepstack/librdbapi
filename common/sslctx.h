/***********************************************************************
* COPYRIGHT (C) 2018 PEPSTACK, PEPSTACK.COM
*
* THIS SOFTWARE IS PROVIDED 'AS-IS', WITHOUT ANY EXPRESS OR IMPLIED
* WARRANTY. IN NO EVENT WILL THE AUTHORS BE HELD LIABLE FOR ANY DAMAGES
* ARISING FROM THE USE OF THIS SOFTWARE.
*
* PERMISSION IS GRANTED TO ANYONE TO USE THIS SOFTWARE FOR ANY PURPOSE,
* INCLUDING COMMERCIAL APPLICATIONS, AND TO ALTER IT AND REDISTRIBUTE IT
* FREELY, SUBJECT TO THE FOLLOWING RESTRICTIONS:
*
*  THE ORIGIN OF THIS SOFTWARE MUST NOT BE MISREPRESENTED; YOU MUST NOT
*  CLAIM THAT YOU WROTE THE ORIGINAL SOFTWARE. IF YOU USE THIS SOFTWARE
*  IN A PRODUCT, AN ACKNOWLEDGMENT IN THE PRODUCT DOCUMENTATION WOULD
*  BE APPRECIATED BUT IS NOT REQUIRED.
*
*  ALTERED SOURCE VERSIONS MUST BE PLAINLY MARKED AS SUCH, AND MUST NOT
*  BE MISREPRESENTED AS BEING THE ORIGINAL SOFTWARE.
*
*  THIS NOTICE MAY NOT BE REMOVED OR ALTERED FROM ANY SOURCE DISTRIBUTION.
***********************************************************************/

/**
 * @file: sslctx.h
 *   openssl api wrapper for both Linux and Windows
 *
 * @author: master@pepstack.com
 *
 * @version: 1.8.8
 *
 * @create: 2015-07-21
 *
 * @update: 2019-06-28
 *
 *----------------------------------------------------------------------
 * api doc:
 *   http://openssl.org/docs/manmaster/ssl/SSL_CTX_use_certificate.html
 *
 * reference:
 *   0) https://www.cnblogs.com/lsdb/p/9391979.html
 *   1) http://www.cppblog.com/flyonok/archive/2011/03/24/133100.html
 *   2) http://www.cppblog.com/flyonok/archive/2010/10/30/131840.html
 *   3) http://www.linuxidc.com/Linux/2011-04/34523.htm
 *   4) http://blog.csdn.net/demetered/article/details/12511771
 *   5) http://zhoulifa.bokee.com/6074014.html
 *   6) http://blog.csdn.net/jinhill/article/details/6960874
 *   7) http://www.ibm.com/developerworks/cn/linux/l-openssl.html
 *   8) http://linux.chinaunix.net/docs/2006-11-10/3175.shtml
 *   9) http://www.cnblogs.com/huhu0013/p/4791724.html
 *----------------------------------------------------------------------
 */

#ifndef SSLCTX_H_INCLUDED
#define SSLCTX_H_INCLUDED

#if defined(__cplusplus)
extern "C" {
#endif

#include "sockapi.h"


/**
 * openssl
 */
#include <openssl/rsa.h>
#include <openssl/crypto.h>
#include <openssl/x509.h>
#include <openssl/pem.h>
#include <openssl/ssl.h>
#include <openssl/err.h>
#include <openssl/rand.h>


#if defined(__WINDOWS__)
 /* download openssl prebuild binary on windows:
  *   https://bintray.com/vszakats/generic/openssl
  */
# ifdef _WIN64
#   pragma comment(lib, "libcrypto-1_1-x64.lib")
#   pragma comment(lib, "libssl-1_1-x64.lib")
# else
#   pragma comment(lib, "libcrypto-1_1.lib")
#   pragma comment(lib, "libssl-1_1.lib")
# endif
#endif


typedef const SSL_METHOD* (*SSL_server_methodP) (void);
typedef const SSL_METHOD* (*SSL_client_methodP) (void);

typedef void (*sslcert_printP) (X509 *, char *, int);


/**
 * ssl nonblock read
 *
 * refer:
 *   https://stackoverflow.com/questions/31171396/openssl-non-blocking-socket-ssl-read-unpredictable
 */
#define SSL_READ_SUCCESS        0
#define SSL_READ_ERROR        (-1)
#define SSL_READ_TIMEOUT      (-2)
#define SSL_READ_PEERCLOSED   (-3)
#define SSL_READ_SSLERROR     (-4)

#define SSL_READ_FINISHED       SSL_READ_SUCCESS
#define SSL_READ_CONTINUE       1
#define SSL_READ_OVERFLOW       2

#define SSL_WRITE_SUCCESS      SSL_READ_SUCCESS
#define SSL_WRITE_ERROR        SSL_READ_ERROR
#define SSL_WRITE_TIMEOUT      SSL_READ_TIMEOUT
#define SSL_WRITE_PEERCLOSED   SSL_READ_PEERCLOSED
#define SSL_WRITE_SSLERROR     SSL_READ_SSLERROR

#define SSLCTX_PATHNAME_LEN_MAX      127
#define SSLCTX_PASSPHRASE_LEN_MAX    40

#define SSLCTX_ERRBUF_LEN_MAX        127

#define SSLCTX_VERIFY_DEPTH_DEFAULT  10

#define SSLCTX_CLIENT_AUTH_NO        0
#define SSLCTX_CLIENT_AUTH_NEED      1


static int sslctx_last_error(char errbuf[SSLCTX_ERRBUF_LEN_MAX + 1])
{
    int err;

    errbuf[0] = 0;

    err = ERR_peek_last_error();
    if (err) {
        // errbuf must be at least 120 bytes long
        ERR_error_string(err, errbuf);
    }
    errbuf[SSLCTX_ERRBUF_LEN_MAX] = 0;

    // 0 - no error
    return err;
}


static const char * ssl_read_errstr (int err)
{
    switch (err) {
    case SSL_READ_SUCCESS:
        return "SUCCESS";
    case SSL_READ_ERROR:
        return "ERROR";
    case SSL_READ_TIMEOUT:
        return "TIMEOUT";
    case SSL_READ_PEERCLOSED:
        return "PEERCLOSED";
    case SSL_READ_SSLERROR:
        return "SSLERROR";
    default:
        return "UNEXCEPT";
    }
}


/**
 * print certificate info
 */
static void sslctx_cert_printf (X509 *cert, char *buf, int bufsz)
{
    char *line;

    /**
     * X509_NAME * X509_get_issuer_name(X509 *cert);
     *
     * X509_NAME * X509_get_subject_name(X509 *a);
     *
     * char * X509_NAME_oneline(X509_NAME *a, char *buf, int size);
     */
    line = X509_NAME_oneline(X509_get_subject_name(cert), buf, bufsz);
    printf("subject=%s\n", line);

    if (line != buf) {
        OPENSSL_free(line);
    }

    line = X509_NAME_oneline(X509_get_issuer_name(cert), buf, bufsz);
    printf("issuer=%s\n", line);

    if (line != buf) {
        OPENSSL_free(line);
    }
}


/**
 * server_sslctx_create()
 *   create SSL_CTX for server side
 *
 * - server_method: NULL or SSLv23_server_method
 * - verify_mode:
 *   SSL_CTX_set_verify()
 *     https://blog.csdn.net/u013919153/article/details/78616737
 *
 *   SSL_VERIFY_NONE
 *     表示不验证
 *
 *   SSL_VERIFY_PEER
 *     用于客户端时要求服务器必须提供证书
 *     用于服务器时服务器会发出证书请求消息要求客户端提供证书, 但是客户端也可以不提供
 *
 *   SSL_VERIFY_FAIL_IF_NO_PEER_CERT
 *     只适用于服务器且必须提供证书, 必须与 SSL_VERIFY_PEER 一起使用才能双向认证
 *         SSL_VERIFY_PEER | SSL_VERIFY_FAIL_IF_NO_PEER_CERT
 *
 * - verify_depth:  > 0: SSL_VERIFY_PEER with depth; = 0: SSL_VERIFY_NONE (default)
 *
 * - CAfile: pem 格式的 CA 证书文件
 * - CApath: pem 格式的 CA 证书所在的目录
 *
 * cacert_pem_file: CA 的证书 (客户端的 CA 证书, 与服务端为同一 CA 签发)
 * pubcert_pem_file: 服务端公钥证书(需经CA签名)
 * privkey_pem_file: 服务端私钥证书
 * privkey_passphrase: 服务端私钥证书密码
 * sslerr: 输出的 SSL 错误代码
 * errbuf: 输出的错误消息
 *
 */
static SSL_CTX * server_sslctx_create (
    SSL_server_methodP server_method,
    int client_auth,
    int verify_depth,
    const char * CAfile,
    const char * CApath,
    const char * CertPem,
    const char * PrivPem,
    char * passphrase,
    const char * cipher_list,
    int * sslerr,
    char errbuf[SSLCTX_ERRBUF_LEN_MAX + 1])
{
    SSL_CTX * sslctx;

    // 载入所有 SSL 错误消息
    SSL_load_error_strings();

    // SSL 库初始化
    SSL_library_init();

    // 载入所有 SSL 算法
    OpenSSL_add_all_algorithms();

    /**
     * 创建SSL上下文环境 每个进程只需维护一个 SSL_CTX 结构体
     *
     * SSLv23_server_method: 以 SSL V2 和 V3 标准兼容方式产生一个 SSL_CTX
     *   也可以用 SSLv2_server_method() 或 SSLv3_server_method()
     *   单独表示 V2 或 V3标准
     *
     * SSL_METHOD的构造函数包括:
     *   TLSv1_server_method(void);   TLSv1.0
     *   TLSv1_client_method(void);   TLSv1.0
     *   SSLv2_server_method(void);   SSLv2
     *   SSLv2_client_method(void);   SSLv2
     *   SSLv3_server_method(void);   SSLv3
     *   SSLv3_client_method(void);   SSLv3
     *   SSLv23_server_method(void);  SSLv3 but can rollback to v2
     *   SSLv23_client_method(void);  SSLv3 but can rollback to v2
     */
    sslctx = SSL_CTX_new(server_method? server_method() : SSLv23_server_method());

    if (! sslctx) {
        *sslerr = sslctx_last_error(errbuf);
        return NULL;
    }

    // 配置认证方式
    if (client_auth) {
        SSL_CTX_set_verify(sslctx, SSL_VERIFY_PEER | SSL_VERIFY_FAIL_IF_NO_PEER_CERT, NULL);
    } else {
        SSL_CTX_set_verify(sslctx, SSL_VERIFY_NONE, NULL);
    }

    if (CAfile) {
        // 若验证，则放置CA证书 */
        SSL_CTX_set_client_CA_list(sslctx, SSL_load_client_CA_file(CAfile));
    }

    // 设置最大的验证用户证书的上级数
    SSL_CTX_set_verify_depth(sslctx, verify_depth);

    // 设置证书密码
    if (passphrase) {
        SSL_CTX_set_default_passwd_cb_userdata(sslctx, passphrase);
    }

    // 加载信任的根证书
    //   https://www.openssl.org/docs/man1.0.2/man3/SSL_CTX_load_verify_locations.html
    if (SSL_CTX_load_verify_locations(sslctx, ((CAfile && CAfile[0])? CAfile : NULL), ((CApath && CApath[0])? CApath : NULL)) <= 0) {
        *sslerr = sslctx_last_error(errbuf);
        SSL_CTX_free(sslctx);
        return NULL;
    }

    // 加载自己的证书, 此证书(公钥)用来发送给客户端
    if (SSL_CTX_use_certificate_file(sslctx, CertPem, SSL_FILETYPE_PEM) <= 0) {
        *sslerr = sslctx_last_error(errbuf);
        SSL_CTX_free(sslctx);
        return NULL;
    }

    // 加载自己的私钥证书
    if (SSL_CTX_use_PrivateKey_file(sslctx, PrivPem, SSL_FILETYPE_PEM) <= 0) {
        *sslerr = sslctx_last_error(errbuf);
        SSL_CTX_free(sslctx);
        return NULL;
    }

    // 调用了以上两个函数后, 检验一下证书与私钥是否配对
    if (CertPem && PrivPem) {
        if (! SSL_CTX_check_private_key(sslctx)) {
            *sslerr = sslctx_last_error(errbuf);
            SSL_CTX_free(sslctx);
            return NULL;
        }
    }

    if (cipher_list && cipher_list[0]) {
        SSL_CTX_set_cipher_list(sslctx, cipher_list);
    } else {
        SSL_CTX_set_cipher_list(sslctx, "RC4-MD5");
    }

    return sslctx;
}


static SSL_CTX * client_sslctx_create (
    SSL_client_methodP client_method,
    int client_auth,
    int verify_depth,
    const char * CAfile,
    const char * CertPem,
    const char * PrivPem,
    char * passphrase,
    char * CipherList,
    int * sslerr,
    char errbuf[SSLCTX_ERRBUF_LEN_MAX + 1])
{
    SSL_CTX * sslctx;

    // 载入所有 SSL 错误消息
    SSL_load_error_strings();

    // SSL 库初始化
    SSL_library_init();

    // 载入所有 SSL 算法
    OpenSSL_add_all_algorithms();

    sslctx = SSL_CTX_new(client_method? client_method() : SSLv23_client_method());

    if (! sslctx) {
        *sslerr = sslctx_last_error(errbuf);
        return NULL;
    }

    // 配置认证方式
    if (client_auth) {
        SSL_CTX_set_verify(sslctx, SSL_VERIFY_PEER | SSL_VERIFY_FAIL_IF_NO_PEER_CERT, NULL);
    } else {
        SSL_CTX_set_verify(sslctx, SSL_VERIFY_NONE, NULL);
    }

    // 设置最大的验证用户证书的上级数
    SSL_CTX_set_verify_depth(sslctx, verify_depth);

    // 设置证书密码
    if (passphrase) {
        SSL_CTX_set_default_passwd_cb_userdata(sslctx, passphrase);
    }

    // 设置信任根证书
    //   https://www.openssl.org/docs/man1.0.2/man3/SSL_CTX_load_verify_locations.html
    if (SSL_CTX_load_verify_locations(sslctx, CAfile, NULL) <= 0) {
        *sslerr = sslctx_last_error(errbuf);
        SSL_CTX_free(sslctx);
        return NULL;
    }

    // 加载自己的证书, 此证书(公钥)用来发送给客户端
    if (SSL_CTX_use_certificate_file(sslctx, CertPem, SSL_FILETYPE_PEM) <= 0) {
        *sslerr = sslctx_last_error(errbuf);
        SSL_CTX_free(sslctx);
        return NULL;
    }

    // 加载自己的私钥证书
    if (SSL_CTX_use_PrivateKey_file(sslctx, PrivPem, SSL_FILETYPE_PEM) <= 0) {
        *sslerr = sslctx_last_error(errbuf);
        SSL_CTX_free(sslctx);
        return NULL;
    }

    // 检查用户私钥是否正确
    if (! SSL_CTX_check_private_key(sslctx)) {
        *sslerr = sslctx_last_error(errbuf);
        SSL_CTX_free(sslctx);
        return NULL;
    }

    if (CipherList) {
        SSL_CTX_set_cipher_list(sslctx, CipherList);
    } else {
        SSL_CTX_set_cipher_list(sslctx, "RC4-MD5");
    }

    return sslctx;
}


/**
 * only for ssl server
 */
static SSL * server_ssl_accept_new (SSL_CTX *sslctx, SOCKET fd)
{
    SSL *ssl = SSL_new(sslctx);
    if (! ssl) {
        return NULL;
    }

    if (! SSL_set_fd(ssl, (int)fd)) {
        SSL_free(ssl);
        return NULL;
    }

    /* http://www.cnblogs.com/dongfuye/p/4121066.html */
    SSL_set_accept_state(ssl);
    return ssl;
}


/**
 * only for ssl client
 */
static SSL * client_ssl_connect_new (SSL_CTX *sslctx, SOCKET fd, int timeoms)
{
    int rc, err;

    SSL *ssl = SSL_new(sslctx);
    if (! ssl) {
        return NULL;
    }

    if (! SSL_set_fd(ssl, (int)fd)) {
        SSL_free(ssl);
        return NULL;
    }

    SSL_set_connect_state(ssl);

    while ((rc = SSL_do_handshake(ssl)) != 1) {
        err = SSL_get_error(ssl, rc);

        if (err == SSL_ERROR_SYSCALL && ! ERR_get_error()) {
            /* error protocol: openssl on client-side not valid */
            printf("(sslctx.h:%d) SSL_ERROR_SYSCALL\n", __LINE__);
            SSL_free(ssl);
            return NULL;
        }

        if (err == SSL_ERROR_WANT_WRITE) {
            rc = watch_fd_write(fd, timeoms);
        } else if (err == SSL_ERROR_WANT_READ) {
            rc = watch_fd_read(fd, timeoms);
        } else {
            rc = -1;
        }

        if (rc == -1) {
            /* error */
            SSL_free(ssl);
            return NULL;
        }
    }

    /* success */
    return ssl;
}



static int sslctx_verify_sslcert (SSL * ssl, sslcert_printP cert_printf, char *buf, int bufsz)
{
    X509 *cert;

    cert = SSL_get_peer_certificate(ssl);
    if (! cert) {
        return 0;
    }

    if (cert_printf) {
        cert_printf(cert, buf, bufsz);
    }

    X509_free(cert);

    return ((SSL_get_verify_result(ssl) == X509_V_OK)? 1 : 0);
}


static void ssl_delete (SSL *ssl, int state)
{
    /**
     * Refer:
     *   https://github.com/cesanta/mongoose
     *   https://github.com/cesanta/mongoose/issues/168
     *
     * According to openssl lib description, it should run SSL_shutdown()
     *  twice to make sure SSL connection is break up, then run SSL_free.
     */
    if (state) {
        SSL_shutdown(ssl);
        SSL_shutdown(ssl);
    }

    SSL_free(ssl);
}


static int ssl_readmsg_nb (SSL *ssl, char *inbuf, ssize_t bufsz, int timeoms,
    int (* onmsg_cb)(char *, ssize_t *, void *), void *cbarg)
{
    int rc, err, result;

    ssize_t offsz = 0;

    result = SSL_READ_OVERFLOW;

    while (offsz < bufsz) {
        rc = SSL_read(ssl, inbuf + offsz, (int)(bufsz - offsz));

        if (rc > 0) {
            // read success rc bytes
            offsz += rc;

            result = onmsg_cb(inbuf, &offsz, cbarg);

            if (result != SSL_READ_CONTINUE) {
                // notify stop(=0) by caller
                break;
            }
        } else {
            err = SSL_get_error(ssl, rc);

            switch (err) {
            case SSL_ERROR_WANT_READ:
                rc = watch_fd_read(SSL_get_rfd(ssl), timeoms);
                if (rc > 0) {
                    // more data to read
                    continue;
                }

                // watch_fd_read error
                if (rc < 0) {
                    result = SSL_READ_ERROR;
                } else {
                    // 如果对端没有数据, 返回超时
                    result = SSL_READ_TIMEOUT;
                }                
                break;

            case SSL_ERROR_WANT_WRITE:
                rc = watch_fd_write(SSL_get_wfd(ssl), timeoms);
                if (rc > 0) {
                    // can write data now
                    continue;
                }

                // watch_fd_write error
                if (rc < 0) {
                    result = SSL_WRITE_ERROR;
                } else {
                    result = SSL_WRITE_TIMEOUT;
                }
                break;

            case SSL_ERROR_ZERO_RETURN:
                // peer shutdown cleanly
                result = SSL_READ_PEERCLOSED;
                break;

            case SSL_ERROR_NONE:
                // no real error, just try again
                continue;

            default:
                // ssl read error
                result = SSL_READ_SSLERROR;
                break;
            }

            // stop read on ssl error
            break;
        }
    }

    return result;
}


/**
 * ssl nonblock write
 */
static int ssl_writemsg_nb (SSL *ssl, const char *msgbuf, size_t msgsz, int timeoms, off_t *outoffsz)
{
    int rc, err, result;

    off_t offsz = 0;

    result = SSL_WRITE_SUCCESS;

    while ((size_t) offsz < msgsz) {
        rc = SSL_write(ssl, (const void *) (msgbuf + offsz), (int) (msgsz - offsz));

        if (rc > 0) {
            // write success with rc bytes
            offsz += rc;
        } else {
            err = SSL_get_error(ssl, rc);

            switch (err) {
            case SSL_ERROR_WANT_WRITE:
                rc = watch_fd_write(SSL_get_wfd(ssl), timeoms);
                if (rc > 0) {
                    // more data to write
                    continue;
                }

                // watch_fd_write error
                if (rc < 0) {
                    result = SSL_WRITE_ERROR;
                } else {
                    result = SSL_WRITE_TIMEOUT;
                }
                break;

            case SSL_ERROR_WANT_READ:
                rc = watch_fd_read(SSL_get_rfd(ssl), timeoms);
                if (rc > 0) {
                    // more data to read
                    continue;
                }

                // watch_fd_read error
                if (rc < 0) {
                    result = SSL_READ_ERROR;
                } else {
                    result = SSL_READ_TIMEOUT;
                }
                break;

            case SSL_ERROR_ZERO_RETURN:
                // peer shutdown cleanly
                result = SSL_WRITE_PEERCLOSED;
                break;

            case SSL_ERROR_NONE:
                // no real error, just try again
                continue;

            default:
                // ssl read error
                result = SSL_WRITE_SSLERROR;
                break;
            }

            // stop read on ssl error
            break;
        }
    }

    *outoffsz = offsz;
    return result;
}


#if defined(__cplusplus)
}
#endif

#endif /* SSLCTX_H_INCLUDED */
