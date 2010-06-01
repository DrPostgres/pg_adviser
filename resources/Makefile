
DATA = index_advisory.create.sql \
		show_index_advisory.create.sql \
		select_index_advisory.create.sql

ifdef USE_PGXS
PGXS := $(shell pg_config --pgxs)
include $(PGXS)
else
subdir = contrib/pgadviser/resources
top_builddir = ../../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif
