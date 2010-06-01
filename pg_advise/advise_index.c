/*
 * pg_advise: the frontend to the indexadvisor
 *
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <string.h>

#include "libpq-fe.h"
#include "advise_index.h"

#define ADV_MAX_COLS 32

static PGconn *init_connection(const char *dbname, const char *host, int port,
						const char *user, const char *password)
{
	char conn_str[1024];
	PGconn *conn;

	sprintf(conn_str, "dbname = %s host = %s port = %d user = %s password = %s"
						" options = '-c local_preload_libraries=libpg_index_adviser'",
				dbname, host, port, user, password);

	conn = PQconnectdb(conn_str);

	if (PQstatus(conn) != CONNECTION_OK)
	{
		fprintf(stderr, "ERROR: %s", PQerrorMessage(conn));
		return NULL;
	}

	return conn;
}

static int prepare_advisor(PGconn *conn)
{
#if 0
	PGresult *res;

	res = PQexec(conn, "SET enable_advise_index TO true");

	if (res == NULL)
	{
		fprintf(stderr, "%s", PQerrorMessage(conn));
		return -1;
	}

	res = PQexec(conn,
			"DELETE FROM advise_index WHERE backend_pid = pg_backend_pid()");

	if (PQresultStatus(res) != PGRES_COMMAND_OK)
	{
		fprintf(stderr, "ERROR: %s", PQerrorMessage(conn));
		return -1;
	}
	else
		PQclear(res);
#endif
	return 0;
}

static int analyse_workload(PGconn *conn, FILE *file)
{
	PGresult *res;
	char *query = NULL;
	char line[1024];

	printf("Analyzing queries ");

	for(;;)
	{
		if (fgets(line, 1024, file) == NULL)
			break;
		if (query == NULL)
		{
			query = (char *)malloc(10*1024);
			strcpy(query, "EXPLAIN ");
			strcat(query, line);
		}
		else
		{
			if (strlen(query) + strlen(line) > 10*1024)
			{
				fprintf(stderr, "ERROR: Query string too long.\n");
				return -1;
			}

			strcat(query, line);
		}

		if (strchr(query, ';') != NULL)
		{
			// printf("query \#%d: %s\n", ++lno, query);
			res = PQexec(conn, query);
			printf(".");
			if (PQresultStatus(res) != PGRES_TUPLES_OK)
			{
				fprintf(stderr, "ERROR: %s", PQerrorMessage(conn));
				return -1;
			}
			else
				PQclear(res);

			free(query);
			query = NULL;
		}
	}
	printf(" done.\n");
	return 0;
}

static int read_advisor_output(PGconn *conn, AdvIndexList *index_list)
{
	PGresult *res;
	int i;
	int num_indexes = 0;
	char stmt[1024];

	res = PQexec(conn, "BEGIN");
	if (PQresultStatus(res) != PGRES_COMMAND_OK)
	{
		fprintf(stderr, "ERROR: BEGIN failed:\n %s", PQerrorMessage(conn));
		PQclear(res);
		return 0;
	}

	snprintf(stmt,	sizeof(stmt),
				"SELECT	c.relname,"
						"attrs AS colids,"
						"MAX(index_size) AS size_in_pages,"
						"SUM(profit) AS benefit,"
						"SUM(profit)/MAX(index_size) AS gain "
				"FROM	advise_index a,"
						"pg_class c "
				"WHERE	a.backend_pid = pg_backend_pid() "
				"AND	a.reloid = c.oid "
				"GROUP BY	c.relname, colids "
				"ORDER BY	gain"
				"	DESC");

	res = PQexec(conn, stmt);
	if (PQresultStatus(res) != PGRES_TUPLES_OK)
	{
		fprintf(stderr, "ERROR: %s", PQerrorMessage(conn));
		PQclear(res);
		return 0;
	}

	*index_list = (AdvIndexInfo **)malloc(PQntuples(res)*sizeof(AdvIndexInfo*));

	for (i = 0; i < PQntuples(res); ++i)
	{
		AdvIndexInfo *index = (AdvIndexInfo *)malloc(sizeof(AdvIndexInfo));

		index->table	= strdup(PQgetvalue(	res, i, 0));
		index->col_ids	= strdup(PQgetvalue(	res, i, 1));
			/*
			 * size returned by the query is in number of pages.
			 * TODO: change the backend to dump size in KBs. Done.
			 */
		index->size		= atol(PQgetvalue(	res, i, 2));
		index->benefit	= atof(PQgetvalue(	res, i, 3));
		index->used		= false;

		(*index_list)[i] = index;

		printf("size = %d KB, benefit = %f\n", index->size, index->benefit);
		++num_indexes;
	}

	PQclear(res);
	res = PQexec(conn, "END");
	PQclear(res);

	return num_indexes;
}

static char* get_column_names(PGconn *conn, const char *table, char *column_ids)
{
	PGresult *res;
	int len, colno;
	char stmt[512];
	char *idxdef;
	char *colnames[ADV_MAX_COLS] = {0};
	char *tok;

	res = PQexec(conn, "BEGIN");

	if (PQresultStatus(res) != PGRES_COMMAND_OK)
	{
		fprintf(stderr, "ERROR: BEGIN failed.\n %s", PQerrorMessage(conn));
		PQclear(res);
		return NULL;
	}

	for(	colno = 0, tok = strtok(column_ids, "{,}");
			tok;
			++colno, tok = strtok(NULL, "{,}"))
	{
		snprintf(stmt, sizeof(stmt),
					"SELECT	a.attname,"
							"a.attnum "
					"FROM	pg_class c,"
							"pg_attribute a "
					"WHERE	c.relname = '%s' "
					"AND	a.attrelid = c.oid "
					"AND	a.attnum = %s",
					table, tok);

		res = PQexec(conn, stmt);
		if (PQresultStatus(res) != PGRES_TUPLES_OK)
		{
			fprintf(stderr, "ERROR: %s", PQerrorMessage(conn));
			PQclear(res);
			return NULL;
		}

		if (PQntuples(res) != 1)
		{
			fprintf(stderr, "ERROR: an internal query failed at %s:%d.",
								__FILE__, __LINE__ );
			PQclear(res);
			return NULL;
		}

		colnames[colno] = strdup(PQgetvalue(res, 0, 0));
	}

	res = PQexec(conn, "END");
	PQclear(res);

	len = 1;	/* 1 for null terminator */
	for (colno = 0; colno < ADV_MAX_COLS && colnames[colno]; ++colno)
	{
		len += strlen(colnames[colno]);
		if (colno > 0) len += 1; /* for a ',' */
	}

	idxdef = (char *)malloc(len);
	idxdef[0] = '\0';

	for (colno = 0; colno < ADV_MAX_COLS && colnames[colno]; ++colno)
	{
		if (colno > 0) strcat(idxdef, ",");
		strcat(idxdef, colnames[colno]);
		free(colnames[colno]);
	}

	return idxdef;
}

static void output_recommendation(PGconn *conn, AdvIndexList index_list,
									int len, FILE *sqlfile)
{
	int i;
	long size = 0;

	for (i = 0; i < len; ++i)
	{
		AdvIndexInfo *info = index_list[i];

		if (!info->used)
			continue;

		char *idxdef = get_column_names(conn, info->table, info->col_ids);

		printf("/* %d. %s(%s): size=%d KB, profit=%.2f */\n",
				i+1, info->table, idxdef, info->size, info->benefit);

		size += info->size;

		if (sqlfile)
			fprintf(sqlfile, "create index idx_%s_%d on %s (%s);\n",
								info->table, i+1, info->table, idxdef);
		free(idxdef);
	}

	printf("/* Total size = %ldKB */\n", size);
}

static void usage()
{
	puts("This is pg_advise_index, the PostgreSQL index advisor frontend.\n");
	puts("Usage:\n\tadvise_index [options] [workload file]\n");
	puts("Options:");
	puts("\t-d DBNAME   specify database name to connect to");
	puts("\t-h HOSTNAME database server host or socket directory "
			"(default: \"local socket\")");
	puts("\t-p PORT     database server port");
	puts("\t-U NAME     database user name");
	puts("\t-o FILENAME name of output file for create index statements");
	puts("\t-s SIZE     specify max size of space to be used for indexes "
			"(in bytes, opt. with G, M or K)");
}

/* return the size (-s option) converted into KBs */
static long strtosize(const char *s)
{
	long size = 0;
	char l = s[strlen(s) - 1];

	if (l == 'G' || l == 'M' || l == 'k' || l == 'K')
	{
		char *ns = (char *)malloc(strlen(s) - 1);
		strncpy(ns, s, strlen(s) - 1);
		size = atol(ns);
		free(ns);
	}

	switch(l)
	{
		case 'G':
			size *= 1024;
		case 'M':
			size *= 1024;
		case 'k':
		case 'K':
			break;
		default:
			size /= 1024;
	}

	return size;
}

int main(int argc, char **argv)
{
	char	*dbname		= NULL,
			*host		= NULL,
			*user		= NULL,
			*password	= NULL;

	int		port = 5432;
	PGconn	*conn;
	long	pool_size = 0;
	FILE	*workload = stdin,
			*sqlfile = NULL;
	int		i,
			num_indexes;
	char	*output_filename = NULL;

	AdvIndexList	suggested_indexes;

	/* check arguments */
	int ch;

	while ((ch = getopt(argc, argv, "d:h:p:U:s:o:W:")) != -1)
		switch(ch)
		{
			case 'd': /* database name */
				dbname = optarg;
				break;
			case 'h': /* database server host */
				host = optarg;
				break;
			case 'p': /* port */
				port = atoi(optarg);
				break;
			case 'U': /* username */
				user = optarg;
				break;
			case 's': /* index pool size */
				pool_size = strtosize(optarg);
				printf("poolsize = %ld KB\n", pool_size);
				break;
			case 'o': /* output file */
				output_filename = optarg;
				break;
			case 'W': /* TODO: prompt for password */
				break;
			case '?':
				usage();
				return 0;
				break;
			default:
				usage();
				return 1;
				break;
		}

	if (dbname == NULL || user == NULL)
	{
		usage();
		return 1;
	}

	argc -= optind;
	argv += optind;

	if (argc == 1)
	{
		workload = fopen(argv[argc-1], "r");

		if (workload == NULL)
		{
			fprintf(stderr, "ERROR: cannot open file %s\n", argv[argc-1]);
			return 1;
		}

		printf("load workload from file '%s'\n", argv[argc-1]);
	}

	/* connect to the backend */
	conn = init_connection(dbname, host, port, user, password);

	if (conn == NULL)
		return 1;

	if (prepare_advisor(conn) != 0)
	{
		fprintf(stderr, "ERROR: this PostgreSQL server doesn't support the "
							"index advisor.\n");
		PQfinish(conn);
		return 1;
	}

	analyse_workload(conn, workload);

	if (workload != stdin)
		fclose(workload);

	num_indexes = read_advisor_output(conn, &suggested_indexes);

	if (pool_size > 0 &&
			compute_config_size(suggested_indexes, num_indexes) > pool_size)
	{
		find_optimal_configuration_greedy(suggested_indexes, num_indexes,
											pool_size / 4);
	}
	else
	{
		for (i = 0; i < num_indexes; ++i)
		suggested_indexes[i]->used = true;
	}

	if (output_filename != NULL)
		sqlfile = fopen(output_filename, "w");
	else
		sqlfile = stdout;

	output_recommendation(conn, suggested_indexes, num_indexes, sqlfile);

	if (output_filename != NULL)
		fclose(sqlfile);

	PQfinish(conn);

	return 0;
}
