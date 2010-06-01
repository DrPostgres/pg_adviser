
create function show_index_advisory(p_backend_pid index_advisory.backend_pid%type) returns text
as
$$
declare
	pid       p_backend_pid%type;
	q_advice  text;
	r_advice  record;
	q_column  text;
	r_column  record;
	ret       text;

	NAMEDATALEN	int := 64;

	collist_w_C		text;	/* Column name list with commas */
	collist_w_U		text;	/* Column name list with underscores */
	colidlist_w_U	text;	/* Column id list with underscores */
begin
	if p_backend_pid is null then
		pid = pg_backend_pid();
	else
		pid = p_backend_pid;
	end if;

	ret :=	'/* Index Adviser */' || E'\n' ||
			'/* ============= */' || E'\n';

	q_advice :=	'SELECT	c.relname,
						c.oid as reloid,
						a.attrs AS colids,
						MAX( a.index_size ) AS size_in_KB,
						SUM( a.profit ) AS benefit,
						SUM( a.profit )/MAX( a.index_size ) AS gain
				FROM    index_advisory a,
						pg_class c
				WHERE   a.backend_pid = ' || pid || '
				AND     a.reloid = c.oid
				GROUP BY    c.relname, c.oid, a.attrs
				ORDER BY    gain
					DESC';
					
	for r_advice in execute q_advice loop

		ret := ret ||
				E'\n/* size: ' || r_advice.size_in_KB || ' KB, '
				|| 'benefit: ' || r_advice.benefit || ', '
				|| 'gain: ' || r_advice.gain || E' */\n';

		collist_w_C		:= '';
		collist_w_U     := '';
		colidlist_w_U	:= '';

		for i in array_lower( r_advice.colids, 1 )
					.. array_upper( r_advice.colids, 1 )
		loop

			q_column :=	'SELECT a.attname as name,
								a.attnum as id
						FROM    pg_class c,
								pg_attribute a
						WHERE   c.oid = ' || r_advice.reloid || '
						AND     a.attrelid = c.oid
						AND     a.attnum = ' || r_advice.colids[i] || '
						';

			execute q_column into r_column;

--			if ROW_COUNT > 1 then
--				raise EXCEPTION 'an internal query failed';
--			end if;

			if i <> 1 then
				collist_w_C		:= collist_w_C		|| ', ';
				collist_w_U		:= collist_w_U		|| '_';
				colidlist_w_U	:= colidlist_w_U	|| '_';
			end if;

			collist_w_C		:= collist_w_C		|| r_column.name;
			collist_w_U		:= collist_w_U		|| r_column.name;
			colidlist_w_U	:= colidlist_w_U	|| r_column.id;

		end loop;

		ret := ret || 'create index ';

		if (length('idx_' || r_advice.relname || '_' || collist_w_U)
				<= NAMEDATALEN)
		then
			ret := ret || 'idx_' || r_advice.relname	|| '_' || collist_w_U;
		else
			ret := ret || 'idx_' || r_advice.reloid		|| '_' || colidlist_w_U;
		end if;

		ret := ret || ' on ' || r_advice.relname || '(' || collist_w_C || E');\n';

	end loop;

  return ret;
end;
$$ language plpgsql;
