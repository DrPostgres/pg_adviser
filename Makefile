#
# PostgreSQL Adviser top level makefile
#

PGFILEDESC = "PostgreSQL Index Advisor"

ifdef USE_PGXS
PGXS := $(shell pg_config --pgxs)
include $(PGXS)
else
subdir = contrib/adviser
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif


all:
	$(MAKE) -C index_adviser $@
	$(MAKE) -C pg_advise $@
	$(MAKE) -C resources $@
	@echo "PostgreSQL Index Advisor successfully made. Ready to install."

install:
	$(MAKE) -C index_adviser $@
	$(MAKE) -C pg_advise $@
	$(MAKE) -C resources $@
	@echo "PostgreSQL Index Advisor installed."

uninstall:
	$(MAKE) -C index_adviser $@
	$(MAKE) -C pg_advise $@
	$(MAKE) -C resources $@
	@echo "PostgreSQL Index Advisor uninstalled."

clean:
	$(MAKE) -C index_adviser $@
	$(MAKE) -C pg_advise $@
	$(MAKE) -C resources $@
