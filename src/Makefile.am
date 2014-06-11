AM_CFLAGS = -I$(top_srcdir)/src

bin_PROGRAMS = zuid
zuid_SOURCES = zuid.c

AM_CFLAGS += $(ZMQ_CFLAGS)
AM_CFLAGS += $(CZMQ_CFLAGS)

zuid_LDADD = $(ZMQ_LIBS)
zuid_LDADD += $(CZMQ_LIBS)
