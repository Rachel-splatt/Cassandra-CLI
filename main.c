#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include "config.h"
#include <cassandra.h>


char *readline(char *prompt);


static int tty = 0;

static void
cli_about()
{
    printf("You executed a command!\n");
}


CassCluster* create_cluster(){
    char* hosts = "127.0.0.1";
    CassCluster* cluster = cass_cluster_new();
    cass_cluster_set_contact_points(cluster, hosts);
    return cluster;
}

//connects to session
CassError connect_session(CassSession* session, const CassCluster* cluster) {
    CassError rc = CASS_OK;
    CassFuture* future = cass_session_connect(session, cluster);
    
    cass_future_wait(future);
    rc = cass_future_error_code(future);

    cass_future_free(future);
    
    return rc;
}

//function for executing queries

CassIterator *execute_query(CassSession* session, const char* query) {
    CassError rc = CASS_OK;
    CassFuture* future = NULL;
    CassStatement* statement = cass_statement_new(query,0);
    future = cass_session_execute(session, statement);
    cass_statement_free(statement);
    rc = cass_future_error_code(future);

    
    printf("Query result: %s\n", cass_error_desc(rc));
    const CassResult * result = cass_future_get_result(future);
    if (result == NULL) {
        cass_future_free(future);
        return NULL;
    }
    
    cass_future_free(future);
    CassIterator * rows = cass_iterator_from_result(result);
    return rows;
}

static void
cli_help()
{
    return;
}

void
cli( CassSession* session)
{
    
    char * currentTable;
    char * currentKeyspace;
    char *cmdline = NULL;
    char cmd[BUFSIZE], prompt[BUFSIZE];
    int pos;
    
    tty = isatty(STDIN_FILENO);
    if (tty)
        cli_about();
    
    /* Main command line loop */
    for (;;) {
        if (cmdline != NULL) {
            free(cmdline);
            cmdline = NULL;
        }
        memset(prompt, 0, BUFSIZE);
        sprintf(prompt, "cassandra> ");
        
        if (tty)
            cmdline = readline(prompt);
        else
            cmdline = readline("");
        
        if (cmdline == NULL)
            continue;
        
        if (strlen(cmdline) == 0)
            continue;
        
        if (!tty)
            printf("%s\n", cmdline);
        
        if (strcmp(cmdline, "?") == 0) {
            cli_help();
            continue;
        }
        if (strcmp(cmdline, "quit") == 0 ||
            strcmp(cmdline, "q") == 0)
            break;
        
        memset(cmd, 0, BUFSIZE);
        pos = 0;
        nextarg(cmdline, &pos, " ", cmd);
        
        if (strcmp(cmd, "about") == 0 || strcmp(cmd, "a") == 0) {
            cli_about();
            continue;
            
        }
        
        if (strcmp(cmd, "show") == 0 || strcmp(cmd, "s") == 0) {
            
            const char* showquery= "SELECT * FROM system_schema.keyspaces;";
            CassIterator* rows= execute_query(session, showquery);
            
            while (cass_iterator_next(rows)) {
                const char *stringval;
                size_t stringlength;
                
                const CassRow* row = cass_iterator_get_row(rows);
                const CassValue *value = cass_row_get_column_by_name(row, "keyspace_name");
                
                cass_value_get_string(value, &stringval, &stringlength);
                printf("%s\n", stringval);
            }
            cass_iterator_free(rows);
            
            continue;
            
        }
        
        if (strcmp(cmd, "list") == 0 || strcmp(cmd, "l") == 0) {
            
            char listquery[BUFSIZE];
            sprintf(listquery, "select table_name from system_schema.tables where keyspace_name = '%s';", currentKeyspace);
            //printf(listquery);
            //const char* listquery="DESCRIBE TABLES;";
            CassIterator* rows= execute_query(session, listquery);
            
            while (cass_iterator_next(rows)) {
                
                const char *stringval;
                size_t stringlength;
                
                const CassRow* row = cass_iterator_get_row(rows);
                const CassValue *value = cass_row_get_column_by_name(row, "table_name");
                
                cass_value_get_string(value, &stringval, &stringlength);
                printf("%s\n", stringval);
            }
            
            cass_iterator_free(rows);

            continue;
            
        }
        
        if (strcmp(cmd, "use") == 0 || strcmp(cmd, "u") == 0) {
            char keyandtable[BUFSIZE];
            char keyspace[BUFSIZE];
            char table[BUFSIZE];
            nextarg(cmdline, &pos, " ", keyandtable);
            int argument = 0;
            nextarg(keyandtable, &argument, ".", keyspace);
            argument++;
            nextarg(keyandtable, &argument, ".", table);
            
            currentTable = table;
            currentKeyspace = keyspace;
            
            char usequery1[BUFSIZE];
            //SimpleStrategy : Use only for a single data center and one rack.
            sprintf(usequery1, "CREATE KEYSPACE IF NOT EXISTS %s WITH REPLICATION = {'class' : 'SimpleStrategy', 'replication_factor' : 1};", keyspace);
            //printf(usequery1);
            CassResult *result = execute_query(session, usequery1);
            
            char usequery2[BUFSIZE];
            sprintf(usequery2, "CREATE TABLE IF NOT EXISTS %s.%s (key text PRIMARY KEY, value text);", keyspace,table);
            CassResult *result2 = execute_query(session, usequery2);
            
            char usequery3[BUFSIZE];
            sprintf(usequery3, "USE %s;", currentKeyspace);
            //printf(usequery3);
            CassResult *result3 = execute_query(session, usequery3);

            continue;
            
        }
        
//get values by key
        
        if (strcmp(cmd, "get") == 0 || strcmp(cmd, "g") == 0) {
            
            char key[BUFSIZE];
            nextarg(cmdline,&pos," ", key);
            
            char getquery[BUFSIZE];
            sprintf(getquery, "select %s from %s.%s;", key,currentKeyspace,currentTable);
            printf(getquery);
            CassIterator *rows= execute_query(session,getquery);
            
            while (cass_iterator_next(rows)) {
                const char *stringval;
                size_t stringlength;
                
                const CassRow* row = cass_iterator_get_row(rows);
                const CassValue *value = cass_row_get_column_by_name(row, key);
                
                cass_value_get_string(value, &stringval, &stringlength);
                printf("%s\n", stringval);
            }
            continue;
        }
        
//insert key value
        if (strcmp(cmd, "insert") == 0 || strcmp(cmd, "i") == 0) {
        
            char key[BUFSIZE];
            nextarg(cmdline,&pos," ", key);

            char value[BUFSIZE];
            nextarg(cmdline, &pos, " ", value);
            
            char insertquery[BUFSIZE];
            sprintf(insertquery, "insert into %s.%s (%s) VALUES('%s');", currentKeyspace,currentTable,key,value);
            printf(insertquery);
            
            execute_query(session,insertquery);
            continue;
            
        }
        
    }
}

int
main(int argc, char**argv)
{
    CassCluster* cluster = create_cluster();
    CassSession* session = cass_session_new();
    CassFuture* close_future = NULL;
    
    if (connect_session(session, cluster) != CASS_OK) {
        cass_cluster_free(cluster);
        cass_session_free(session);
        return -1;
    }
    
    cli(session);
    exit(0);
}
