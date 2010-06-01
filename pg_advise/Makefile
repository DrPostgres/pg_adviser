
subdir = contrib/pgadviser/pg_advise
top_builddir = ../../..
include $(top_builddir)/src/Makefile.global

PROGRAM = pg_advise
OBJS = advise_index.o util_funcs.o

PG_CPPFLAGS = -I$(libpq_srcdir)
PG_LIBS = $(libpq)

include $(top_srcdir)/contrib/contrib-global.mk
