from snowflake.connector import DictCursor, ProgrammingError
import logging


def run(conn, sql, params=None):
    cur = conn.cursor()
    try:
        cur.execute(sql, params)
    except ProgrammingError:
        raise
    finally:
        cur.close()


def run_and_fetchall(conn, sql, params=None):
    cur = conn.cursor(DictCursor)
    try:
        cur.execute(sql, params)
    except ProgrammingError:
        raise
    else:
        results = cur.fetchall()
    finally:
        cur.close()
    # Lowercase all column names because we use lowercase identifiers everywhere in our SQL code
    return [{k.lower(): v for k, v in rec.items()} for rec in results]


def run_own_connection(get_conn_callback, sql, params=None):
    conn = get_conn_callback()
    run(conn, sql, params)
    conn.close()


def run_and_fetchall_own_connection(get_conn_callback, sql, params=None):
    conn = get_conn_callback()
    rows = run_and_fetchall(conn, sql, params)
    conn.close()
    return rows


def sql_ts_nodash_to_timestamp_ntz(ts_nodash):
    return f"""TO_TIMESTAMP_NTZ({ts_nodash}, 'YYYYMMDD"T"HH24MISS')"""


def sql_surrogate_key(unique_key_columns):
    cols_casted_and_md5ed = [f"MD5(COALESCE(IDENTIFIER('{col}')::VARCHAR, '__EQUAL_NULL__'))" for col in unique_key_columns]
    sql_concatenation = "\n      || '||'\n      ||".join(cols_casted_and_md5ed)
    return f"""MD5(
         {sql_concatenation}
    )"""