#
# PostgreSQL Adviser top level makefile
#

PGFILEDESC = "PostgreSQL Index Advisor"
subdir = contrib/adviser
top_builddir = ../..
include $(top_builddir)/src/Makefile.global

all:
	$(MAKE) -C index_adviser all
	$(MAKE) -C pg_advise all
	$(MAKE) -C resources all
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
