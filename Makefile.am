noinst_LIBRARIES = librdbapi.a


librdbapi_a_SOURCES = common/red_black_tree.c \
	common/tiny-regex-c/re.c \
    rdbactx.c \
    rdbctx.c \
    rdbenv.c \
    rdbmapr.c \
    rdbparam.c \
    rdbstmt.c \
    rdbtable.c \
    rdbexpr.c \
    rdbapi.c


librdbapi_a_LIBADD = $(top_srcdir)/libs/lib/libhiredis.a


DEFS += -D_GNU_SOURCE


AM_CPPFLAGS = -I$(top_srcdir)/src \
    -I$(top_srcdir)/src/librdbapi \
    -I$(top_srcdir)/src/librdbapi/common \
    -I$(top_srcdir)/src/librdbapi/uthash \
    -I$(top_srcdir)/libs/include


#### test c app
noinst_PROGRAMS = test

test_SOURCES = test.c

test_LDADD = ./librdbapi.a \
	$(top_srcdir)/libs/lib/libhiredis.a \
    -lm \
    -lrt \
    -ldl \
    -lpthread
