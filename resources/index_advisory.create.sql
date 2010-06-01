
create table index_advisory(	reloid		oid,
								attrs		integer[],
								profit		real,
								index_size	integer,
								backend_pid	integer,
								timestamp	timestamptz);

create index IA_reloid on index_advisory( reloid );
create index IA_backend_pid on index_advisory( backend_pid );
