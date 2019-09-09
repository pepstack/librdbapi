noinst_LIBRARIES = librdbapi.a


librdbapi_a_SOURCES = common/red_black_tree.c \
	common/tiny-regex-c/re.c \
	common/tpl/tpl.c \
    rdbactx.c \
    rdbctx.c \
    rdbenv.c \
    rdbmapr.c \
    rdbparam.c \
    rdbsqlstmt.c \
    rdbresultmap.c \
    rdbtable.c \
    rdbexpr.c \
    rdbtpl.c \
    rdbapi.c

# static link to hiredis
librdbapi_a_LIBADD = $(top_srcdir)/libs/lib/libhiredis.a


DEFS += -D_GNU_SOURCE


AM_CPPFLAGS = -I$(top_srcdir)/src \
    -I$(top_srcdir)/src/librdbapi \
    -I$(top_srcdir)/src/librdbapi/common \
    -I$(top_srcdir)/src/librdbapi/uthash \
    -I$(top_srcdir)/libs/include


#### redplus app
noinst_PROGRAMS = redplus

redplus_SOURCES = redplus-src/redplusapp.c

redplus_LDADD = ./librdbapi.a \
	$(top_srcdir)/libs/lib/libhiredis.a \
    -lm \
    -lrt \
    -ldl \
    -lpthread
