from snowflake.connector import DictCursor, ProgrammingError
import logging


def test_unique_compound_key(conn, table, columns) -> bool:
    cur = conn.cursor(DictCursor)
    sql = """
SELECT
    {columns_str}
  , COUNT(*) AS n
FROM IDENTIFIER(%(table)s)
GROUP BY
    {columns_str}
HAVING n > 1
ORDER BY N DESC
LIMIT 10
""".format(columns_str='\n  ,'.join([f'IDENTIFIER(%({column})s)' for column in columns]))
    params = {column: column for column in columns}
    params['table'] = table
    try:
        cur.execute(sql, params=params)
    except ProgrammingError as e:
        raise
    else:
        first_up_to_ten_records = cur.fetchall()
    finally:
        cur.close()
    if len(first_up_to_ten_records) > 0:
        logging.warn(f"""
Duplicates found for supposed compound key, using query

{sql}

Worst (up to ten) offenders:

{first_up_to_ten_records}
""")
        return False
    else:
        return True
    