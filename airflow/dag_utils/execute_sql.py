from dag_utils import PostgresManager

def execute_sql(sql_query, log_message="SQL executed successfully"):
    pg = PostgresManager()
    return pg.execute_sql(sql_query, log_message)