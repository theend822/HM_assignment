from dag_utils import PostgresManager

def dq_check_task(check_name, sql_query):
    pg = PostgresManager()
    return pg.run_dq_check(check_name, sql_query)