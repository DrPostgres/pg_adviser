ifdef USE_PGXS
PGXS := $(shell pg_config --pgxs)
include $(PGXS)
else
subdir = contrib/pgadviser/pg_advise
top_builddir = ../../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif

PROGRAM = pg_advise
OBJS = advise_index.o util_funcs.o

PG_CPPFLAGS = -I$(libpq_srcdir)
PG_LIBS = $(libpq)

