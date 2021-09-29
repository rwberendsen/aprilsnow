from snowflake.connector import DictCursor, ProgrammingError
import logging


from util import run_and_fetchall, run, run_and_fetchall_own_connection, run_own_connection
from test_unique_compound_key import test_unique_compound_key
from snapshot_v2 import snapshot, create_scd_table, prep_batch, snapshot_partition, deploy_batch, clean_up_batch


def _is_okay_valid_date_sequence(rows, do_zombie_check):
    rows = sorted(rows, key=lambda x: x['_scd_valid_from_timestamp'])
    if not len(rows):
        return True
    last_row = rows.pop(0)
    while rows:
        current_row = rows.pop(0)
        if last_row['_scd_valid_to_timestamp'] is None:
            if not do_zombie_check:
                if last_row['_scd_deleted_timestamp'] is None:
                    return False
            else:
                return False
        else:
            if current_row['_scd_valid_from_timestamp'] < last_row['_scd_valid_to_timestamp']:
                return False  # no overlaps
            if current_row['_scd_valid_from_timestamp'] > last_row['_scd_valid_to_timestamp']:
                return False # no gaps, either
        last_row = current_row
    return True


def _is_okay_rows_are_different(rows):
    rows = sorted(rows, key=lambda x: x['_scd_valid_from_timestamp'])
    if not len(rows):
        return True
    cols_to_compare = frozenset(rows[0].keys()) - frozenset(['_scd_valid_from_timestamp', '_scd_valid_to_timestamp', '_scd_is_most_recent', '_scd_id',
                                                             '_scd_normalised_key', '_scd_deleted_timestamp', '_scd_created_timestamp', '_scd_last_modified_timestamp',
                                                             '_scd_created_by', '_scd_last_modified_by', '_scd_last_dml_action'])
    last_row = rows.pop(0)
    while rows:
        current_row = rows.pop(0)
        if all([current_row[col] == last_row[col] for col in cols_to_compare]) and last_row['_scd_deleted_timestamp'] is None:
            return False
        last_row = current_row
    return True
    

def _is_okay_invariants_scd_table(rows, do_zombie_check=True):
    """Stuff that always has to hold, looking at the scd table alone. These are candidates for DBT tests on real data as well, although then, they might take a while to run."""
    is_scd_id_unique = (len(frozenset([row['_scd_id'] for row in rows])) == len(rows))
    if not is_scd_id_unique:
        return False

    is_most_recent_agrees_with_valid_to = all([
            (row['_scd_is_most_recent'] and row['_scd_valid_to_timestamp'] is None) \
            or \
            (not row['_scd_is_most_recent'] and row['_scd_valid_to_timestamp'] is not None) for row in rows
    ])
    if not is_most_recent_agrees_with_valid_to:
        return False

    most_recent_rows = [row for row in rows if row['_scd_is_most_recent']]
    if not do_zombie_check:
        most_recent_rows = [row for row in most_recent_rows if row['_scd_deleted_timestamp'] is None]
    is_normalised_key_unique_in_most_recent_rows = (len(frozenset([row['_scd_normalised_key'] for row in most_recent_rows])) == len(most_recent_rows))
    if not is_normalised_key_unique_in_most_recent_rows:
        return False

    is_all_non_null_valid_from = not any([row['_scd_valid_from_timestamp'] is None for row in rows])
    if not is_all_non_null_valid_from:
        return False

    normalised_keys = frozenset([row['_scd_normalised_key'] for row in rows])
    is_okay_valid_date_sequences = all([_is_okay_valid_date_sequence([row for row in rows if row['_scd_normalised_key'] == key], do_zombie_check) for key in normalised_keys])
    if not is_okay_valid_date_sequences:
        return False

    is_okay_rows_are_different = all([_is_okay_rows_are_different([row for row in rows if row['_scd_normalised_key'] == key]) for key in normalised_keys])
    if not is_okay_valid_date_sequences:
        return False

    is_deleted_always_later_than_valid_from = all([row['_scd_deleted_timestamp'] is None or row['_scd_deleted_timestamp'] > row['_scd_valid_from_timestamp'] for row in rows])
    if not is_deleted_always_later_than_valid_from:
        print('deleted from not later than valid_from')
        return False

    is_valid_to_always_later_than_valid_from = all([row['_scd_valid_to_timestamp'] is None or row['_scd_valid_to_timestamp'] > row['_scd_valid_from_timestamp'] for row in rows])
    if not is_valid_to_always_later_than_valid_from:
        print('valid_to not later than valid from')
        return False

    return True


def create_tables(get_conn_callback, database, schema, ts_nodash_col, non_ts_nodash_columns, partition_columns=None):
    drop_tables(get_conn_callback, database, schema)  # in case previous test exited with error before cleaning up;
    columns_str = '\n ,      '.join([f'{col} {type_}' for col, type_ in non_ts_nodash_columns.items()])
    conn = get_conn_callback()
    run(conn, 'USE DATABASE IDENTIFIER(%(database)s);', params={'database': database})
    run(conn, 'USE SCHEMA IDENTIFIER(%(schema)s);', params={'schema': schema})
    # Create input table
    run(conn, f"""
    CREATE OR REPLACE TABLE tmp_in (
        {ts_nodash_col} VARCHAR NOT NULL
      , {columns_str}
      , _extracted_timestamp TIMESTAMP_NTZ NOT NULL DEFAULT SYSDATE() -- just here to test that it won't be used by any of the functions
    )
    ;""")
    conn.close()

    column_types = [(col, type_) for col, type_ in non_ts_nodash_columns.items()] + [(ts_nodash_col, 'VARCHAR NOT NULL')]
    scd_table = f'{database}.{schema}.tmp_scd'
    batch_metadata_table = f'{database}.{schema}.tmp_batch_metadata'
    create_scd_table(get_conn_callback, column_types, ts_nodash_col, scd_table, batch_metadata_table, partition_columns=partition_columns)  # does a create if not exists


def drop_tables(get_conn_callback, database, schema):
    conn = get_conn_callback()
    run(conn, 'USE DATABASE IDENTIFIER(%(database)s);', params={'database': database})
    run(conn, 'USE SCHEMA IDENTIFIER(%(schema)s);', params={'schema': schema})

    # Drop input table
    run(conn, f"""
    DROP TABLE IF EXISTS tmp_in
    ;
    """)

    # Drop target SCD table
    run(conn, f"""
    DROP TABLE IF EXISTS tmp_scd
    ;
    """)

    # Drop target batch metadata table
    run(conn, f"""
    DROP TABLE IF EXISTS tmp_batch_metadata
    ;
    """)

    conn.close()


 
def is_scenario_1_success(database, schema, get_conn_callback) -> bool:
    create_tables(get_conn_callback, database, schema, 't', {'x': 'INT', 'y': 'INT'})
    run_own_connection(get_conn_callback, f"INSERT INTO {database}.{schema}.tmp_in (t, x, y) VALUES ('20210609T050000', 1, 2), ('20210609T050000', 2, 1);")
    snapshot(
            get_conn_callback,
            f'{database}.{schema}',
            'tmp_in',
            f'{database}.{schema}.tmp_in',
            f'{database}.{schema}.tmp_scd',
            f'{database}.{schema}.tmp_batch_metadata',
            'txy',
            'xy',
            't',
            '20210609T050000'
    )
    rows = run_and_fetchall_own_connection(get_conn_callback, f'SELECT * FROM {database}.{schema}.tmp_scd')
    drop_tables(get_conn_callback, database, schema)
    print(rows)
    is_two_rows = (len(rows) == 2)
    is_most_recent_always_true = all([row['_scd_is_most_recent'] for row in rows])
    return _is_okay_invariants_scd_table(rows) and is_two_rows and is_most_recent_always_true 


def is_scenario_2_success(database, schema, get_conn_callback) -> bool:
    create_tables(get_conn_callback, database, schema, 't', {'x': 'INT', 'y': 'INT'})
    # introduce a duplicate row in the source table; only during the second step, where the changes are staged, will it be deduplicated.
    run_own_connection(get_conn_callback, f"INSERT INTO {database}.{schema}.tmp_in (t, x, y) VALUES ('20210609T050000', 1, 2), ('20210609T050000', 2, 1), ('20210609T050000', 2, 1);")
    snapshot(
            get_conn_callback,
            f'{database}.{schema}',
            'tmp_in',
            f'{database}.{schema}.tmp_in',
            f'{database}.{schema}.tmp_scd',
            f'{database}.{schema}.tmp_batch_metadata',
            'txy',
            'xy',
            't',
            '20210609T050000',
            fail_on_duplicates=False
    )
    rows = run_and_fetchall_own_connection(get_conn_callback, f'SELECT * FROM {database}.{schema}.tmp_scd')
    drop_tables(get_conn_callback, database, schema)
    print(rows)
    is_two_rows = (len(rows) == 2)
    is_most_recent_always_true = all([row['_scd_is_most_recent'] for row in rows])
    return _is_okay_invariants_scd_table(rows) and is_two_rows and is_most_recent_always_true


def is_scenario_3_success(database, schema, get_conn_callback) -> bool:
    create_tables(get_conn_callback, database, schema, 't', {'x': 'INT', 'y': 'INT'}, partition_columns=('x',))
    run_own_connection(get_conn_callback, f"INSERT INTO {database}.{schema}.tmp_in (t, x, y) VALUES ('20210609T050000', 1, 1), ('20210609T050000', 1, 2), ('20210609T050000', 2, 1);")
    # use partitions
    snapshot(
            get_conn_callback,
            f'{database}.{schema}',
            'tmp_in',
            f'{database}.{schema}.tmp_in',
            f'{database}.{schema}.tmp_scd',
            f'{database}.{schema}.tmp_batch_metadata',
            'txy',
            'xy',
            't',
            '20210609T050000',
            partitions=({'x': 1},)
    )
    rows = run_and_fetchall_own_connection(get_conn_callback, f'SELECT * FROM {database}.{schema}.tmp_scd')
    drop_tables(get_conn_callback, database, schema)
    print(rows)
    is_two_rows = (len(rows) == 2)
    is_most_recent_always_true = all([row['_scd_is_most_recent'] for row in rows])
    return _is_okay_invariants_scd_table(rows) and is_two_rows and is_most_recent_always_true


def is_scenario_4_success(database, schema, get_conn_callback) -> bool:
    create_tables(get_conn_callback, database, schema, 't', {'x': 'INT', 'y': 'INT'})
    run_own_connection(get_conn_callback, f"INSERT INTO {database}.{schema}.tmp_in (t, x, y) VALUES ('20210609T050000', 1, 2), ('20210609T050000', 2, 1);")
    # use a column with values (y) that is not part of a unique key
    snapshot(
            get_conn_callback,
            f'{database}.{schema}',
            'tmp_in',
            f'{database}.{schema}.tmp_in',
            f'{database}.{schema}.tmp_scd',
            f'{database}.{schema}.tmp_batch_metadata',
            'txy',
            'x',
            't',
            '20210609T050000'
    )
    rows = run_and_fetchall_own_connection(get_conn_callback, f'SELECT * FROM {database}.{schema}.tmp_scd')
    drop_tables(get_conn_callback, database, schema)
    print(rows)
    is_two_rows = (len(rows) == 2)
    is_most_recent_always_true = all([row['_scd_is_most_recent'] for row in rows])
    return _is_okay_invariants_scd_table(rows) and is_two_rows and is_most_recent_always_true


def is_scenario_5_success(database, schema, get_conn_callback) -> bool:
    create_tables(get_conn_callback, database, schema, 't', {'x': 'INT', 'y': 'INT'})
    # now in this table there are two batches worth of data
    run_own_connection(get_conn_callback, f"INSERT INTO {database}.{schema}.tmp_in (t, x, y) VALUES ('20210609T050000', 1, 2), ('20210609T050000', 2, 1), "
              "('20210614T050000', 1, 1), ('20210614T050000', 2, 1);")
    snapshot(
            get_conn_callback,
            f'{database}.{schema}',
            'tmp_in',
            f'{database}.{schema}.tmp_in',
            f'{database}.{schema}.tmp_scd',
            f'{database}.{schema}.tmp_batch_metadata',
            'txy',
            'x',
            't',
            '20210609T050000'
    )
    # a second round to stage the second batch 
    snapshot(
            get_conn_callback,
            f'{database}.{schema}',
            'tmp_in',
            f'{database}.{schema}.tmp_in',
            f'{database}.{schema}.tmp_scd',
            f'{database}.{schema}.tmp_batch_metadata',
            'txy',
            'x',
            't',
            '20210614T050000'
    )
    rows = run_and_fetchall_own_connection(get_conn_callback, f'SELECT * FROM {database}.{schema}.tmp_scd')
    drop_tables(get_conn_callback, database, schema)
    print(rows)
    is_three_rows = (len(rows) == 3)
    is_most_recent_not_always_true = not all([row['_scd_is_most_recent'] for row in rows])
    return _is_okay_invariants_scd_table(rows) and is_three_rows and is_most_recent_not_always_true


def is_scenario_6_success(database, schema, get_conn_callback) -> bool:
    create_tables(get_conn_callback, database, schema, 't', {'x': 'INT', 'y': 'INT'})
    # introduce a deletion in the second batch (remember, each batch is considered to be a full load)
    run_own_connection(get_conn_callback, f"INSERT INTO {database}.{schema}.tmp_in (t, x, y) VALUES ('20210609T050000', 1, 2), ('20210609T050000', 2, 1), "
              "('20210614T050000', 1, 1);")
    snapshot(
            get_conn_callback,
            f'{database}.{schema}',
            'tmp_in',
            f'{database}.{schema}.tmp_in',
            f'{database}.{schema}.tmp_scd',
            f'{database}.{schema}.tmp_batch_metadata',
            'txy',
            'x',
            't',
            '20210609T050000'
    )
    # a second round to stage the second batch 
    snapshot(
            get_conn_callback,
            f'{database}.{schema}',
            'tmp_in',
            f'{database}.{schema}.tmp_in',
            f'{database}.{schema}.tmp_scd',
            f'{database}.{schema}.tmp_batch_metadata',
            'txy',
            'x',
            't',
            '20210614T050000'
    )
    rows = run_and_fetchall_own_connection(get_conn_callback, f'SELECT * FROM {database}.{schema}.tmp_scd')
    drop_tables(get_conn_callback, database, schema)
    print(rows)
    is_three_rows = (len(rows) == 3)
    is_most_recent_not_always_true = not all([row['_scd_is_most_recent'] for row in rows])
    is_a_deleted_row_present = any([row['_scd_deleted_timestamp'] for row in rows])
    return _is_okay_invariants_scd_table(rows) and is_three_rows and is_most_recent_not_always_true and is_a_deleted_row_present


def is_scenario_7_success(database, schema, get_conn_callback) -> bool:
    create_tables(get_conn_callback, database, schema, 't', {'x': 'INT', 'y': 'INT'})
    # introduce a deletion in the second batch (remember, each batch is considered to be a full load)
    # then, in the third batch, re-introduce a previously deleted row, without altering any of the values
    run_own_connection(get_conn_callback, f"INSERT INTO {database}.{schema}.tmp_in (t, x, y) VALUES ('20210609T050000', 1, 2), ('20210609T050000', 2, 1), "
              "('20210614T050000', 1, 1), "
              "('20210619T050000', 1, 1), ('20210619T050000', 2, 1);")
    snapshot(
            get_conn_callback,
            f'{database}.{schema}',
            'tmp_in',
            f'{database}.{schema}.tmp_in',
            f'{database}.{schema}.tmp_scd',
            f'{database}.{schema}.tmp_batch_metadata',
            'txy',
            'x',
            't',
            '20210609T050000'
    )
    # a second round to stage the second batch 
    snapshot(
            get_conn_callback,
            f'{database}.{schema}',
            'tmp_in',
            f'{database}.{schema}.tmp_in',
            f'{database}.{schema}.tmp_scd',
            f'{database}.{schema}.tmp_batch_metadata',
            'txy',
            'x',
            't',
            '20210614T050000'
    )
    # a third round to stage the third batch
    snapshot(
            get_conn_callback,
            f'{database}.{schema}',
            'tmp_in',
            f'{database}.{schema}.tmp_in',
            f'{database}.{schema}.tmp_scd',
            f'{database}.{schema}.tmp_batch_metadata',
            'txy',
            'x',
            't',
            '20210619T050000'
    )
    rows = run_and_fetchall_own_connection(get_conn_callback, f'SELECT * FROM {database}.{schema}.tmp_scd')
    drop_tables(get_conn_callback, database, schema)
    print(rows)
    is_four_rows = (len(rows) == 4)
    is_most_recent_not_always_true = not all([row['_scd_is_most_recent'] for row in rows])
    is_a_deleted_row_present = any([row['_scd_deleted_timestamp'] for row in rows])
    is_a_deleted_invalidated_row_present = any([row['_scd_deleted_timestamp'] and not row['_scd_is_most_recent'] for row in rows])
    return _is_okay_invariants_scd_table(rows) and is_four_rows and is_most_recent_not_always_true and is_a_deleted_row_present and is_a_deleted_invalidated_row_present


def is_scenario_8_success(database, schema, get_conn_callback) -> bool:
    create_tables(get_conn_callback, database, schema, 't', {'x': 'INT', 'y': 'INT'})
    # introduce NULL values in the values (y) and make sure it is not seen as an update (NULL = NULL evaluates to NULL in SQL)
    run_own_connection(get_conn_callback, f"INSERT INTO {database}.{schema}.tmp_in (t, x, y) VALUES ('20210609T050000', 1, NULL), ('20210614T050000', 1, NULL);")
    snapshot(
            get_conn_callback,
            f'{database}.{schema}',
            'tmp_in',
            f'{database}.{schema}.tmp_in',
            f'{database}.{schema}.tmp_scd',
            f'{database}.{schema}.tmp_batch_metadata',
            'txy',
            'x',
            't',
            '20210609T050000'
    )
    # a second round to stage the second batch 
    snapshot(
            get_conn_callback,
            f'{database}.{schema}',
            'tmp_in',
            f'{database}.{schema}.tmp_in',
            f'{database}.{schema}.tmp_scd',
            f'{database}.{schema}.tmp_batch_metadata',
            'txy',
            'x',
            't',
            '20210614T050000'
    )
    rows = run_and_fetchall_own_connection(get_conn_callback, f'SELECT * FROM {database}.{schema}.tmp_scd')
    drop_tables(get_conn_callback, database, schema)
    print(rows)
    is_one_row = (len(rows) == 1)
    is_most_recent_always_true = all([row['_scd_is_most_recent'] for row in rows])
    return _is_okay_invariants_scd_table(rows) and is_one_row and is_most_recent_always_true


def is_scenario_9_success(database, schema, get_conn_callback) -> bool:
    create_tables(get_conn_callback, database, schema, 't', {'x': 'INT', 'y': 'INT'})
    # introduce NULL values in the unique compound key (x) and make sure it is treated as an actual value
    # (Sadly, in real life tables sometimes have no unique compound key, and we have to include columns that can be null in a "guessed" unique compound key in order to make 
    # rows unique in data observed so far.)
    run_own_connection(get_conn_callback, f"INSERT INTO {database}.{schema}.tmp_in (t, x, y) VALUES ('20210609T050000', NULL, 1);")
    snapshot(
            get_conn_callback,
            f'{database}.{schema}',
            'tmp_in',
            f'{database}.{schema}.tmp_in',
            f'{database}.{schema}.tmp_scd',
            f'{database}.{schema}.tmp_batch_metadata',
            'txy',
            'x',
            't',
            '20210609T050000'
    )
    rows = run_and_fetchall_own_connection(get_conn_callback, f'SELECT * FROM {database}.{schema}.tmp_scd')
    drop_tables(get_conn_callback, database, schema)
    print(rows)
    is_one_row = (len(rows) == 1)
    is_most_recent_always_true = all([row['_scd_is_most_recent'] for row in rows])
    return _is_okay_invariants_scd_table(rows) and is_one_row and is_most_recent_always_true


def is_scenario_10_success(database, schema, get_conn_callback) -> bool:
    create_tables(get_conn_callback, database, schema, 't', {'x': 'INT', 'y': 'INT'})
    # introduce NULL values in the unique compound key (x) and make sure it is treated as an actual value
    # (Sadly, in real life tables sometimes have no unique compound key, and we have to include columns that can be null in a "guessed" unique compound key in order to make 
    # rows unique in data observed so far.)
    # Also, add multiple rows with the same NULL compound key, and check that they are deduplicated as a single _scd_normalised_key (with an arbitrary row selected)
    run_own_connection(get_conn_callback, f"INSERT INTO {database}.{schema}.tmp_in (t, x, y) VALUES ('20210609T050000', NULL, 1), ('20210609T050000', NULL, 2);")
    snapshot(
            get_conn_callback,
            f'{database}.{schema}',
            'tmp_in',
            f'{database}.{schema}.tmp_in',
            f'{database}.{schema}.tmp_scd',
            f'{database}.{schema}.tmp_batch_metadata',
            'txy',
            'x',
            't',
            '20210609T050000',
            fail_on_duplicates=False
    )
    rows = run_and_fetchall_own_connection(get_conn_callback, f'SELECT * FROM {database}.{schema}.tmp_scd')
    drop_tables(get_conn_callback, database, schema)
    print(rows)
    is_one_row = (len(rows) == 1)
    is_most_recent_always_true = all([row['_scd_is_most_recent'] for row in rows])
    return _is_okay_invariants_scd_table(rows) and is_one_row and is_most_recent_always_true
    

def is_scenario_11_success(database, schema, get_conn_callback) -> bool:
    create_tables(get_conn_callback, database, schema, 't', {'x': 'INT', 'y': 'INT'})
    # introduce NULL values in the unique compound key (x) and make sure it is treated as an actual value
    # (Sadly, in real life tables sometimes have no unique compound key, and we have to include columns that can be null in a "guessed" unique compound key in order to make 
    # rows unique in data observed so far.)
    # Also, check if we can update rows with a NULL value in a column in the compound key
    run_own_connection(get_conn_callback, f"INSERT INTO {database}.{schema}.tmp_in (t, x, y) VALUES ('20210609T050000', NULL, 1), ('20210614T050000', NULL, 2);")
    snapshot(
            get_conn_callback,
            f'{database}.{schema}',
            'tmp_in',
            f'{database}.{schema}.tmp_in',
            f'{database}.{schema}.tmp_scd',
            f'{database}.{schema}.tmp_batch_metadata',
            'txy',
            'x',
            't',
            '20210609T050000'
    )
    # a second round to stage the second batch 
    snapshot(
            get_conn_callback,
            f'{database}.{schema}',
            'tmp_in',
            f'{database}.{schema}.tmp_in',
            f'{database}.{schema}.tmp_scd',
            f'{database}.{schema}.tmp_batch_metadata',
            'txy',
            'x',
            't',
            '20210614T050000'
    )
    rows = run_and_fetchall_own_connection(get_conn_callback, f'SELECT * FROM {database}.{schema}.tmp_scd')
    drop_tables(get_conn_callback, database, schema)
    print(rows)
    is_two_rows = (len(rows) == 2)
    is_most_recent_not_always_true = not all([row['_scd_is_most_recent'] for row in rows])
    return _is_okay_invariants_scd_table(rows) and is_two_rows and is_most_recent_not_always_true


def is_scenario_12_success(database, schema, get_conn_callback) -> bool:
    create_tables(get_conn_callback, database, schema, 't', {'x': 'INT', 'y': 'INT'}, partition_columns=('x',))
    # Check that partitions are handled okay, and that, if they are used, only rows in scd_table from those partitions will ever be marked for deletion
    run_own_connection(get_conn_callback, f"INSERT INTO {database}.{schema}.tmp_in (t, x, y) VALUES ('20210609T050000', 1, 1), ('20210614T050000', 2, 2);")
    snapshot(
            get_conn_callback,
            f'{database}.{schema}',
            'tmp_in',
            f'{database}.{schema}.tmp_in',
            f'{database}.{schema}.tmp_scd',
            f'{database}.{schema}.tmp_batch_metadata',
            'txy',
            'xy',
            't',
            '20210609T050000',
            partitions=({'x': 1},)
    )
    # a second round to stage the second batch 
    snapshot(
            get_conn_callback,
            f'{database}.{schema}',
            'tmp_in',
            f'{database}.{schema}.tmp_in',
            f'{database}.{schema}.tmp_scd',
            f'{database}.{schema}.tmp_batch_metadata',
            'txy',
            'xy',
            't',
            '20210614T050000',
            partitions=({'x': 2},)
    )
    rows = run_and_fetchall_own_connection(get_conn_callback, f'SELECT * FROM {database}.{schema}.tmp_scd')
    drop_tables(get_conn_callback, database, schema)
    print(rows)
    is_two_rows = (len(rows) == 2)
    is_most_recent_always_true = all([row['_scd_is_most_recent'] for row in rows])
    is_deleted_always_none = all([row['_scd_deleted_timestamp'] is None for row in rows])
    return _is_okay_invariants_scd_table(rows) and is_two_rows and is_most_recent_always_true and is_deleted_always_none


def is_scenario_13_success(database, schema, get_conn_callback) -> bool:
    create_tables(get_conn_callback, database, schema, 't', {'x': 'INT', 'y': 'INT'}, partition_columns=('x', 'y'))
    run_own_connection(get_conn_callback, f"INSERT INTO {database}.{schema}.tmp_in (t, x, y) VALUES ('20210609T050000', 1, 2), ('20210609T050000', 3, 4), ('20210614T050000', 1, 2);")
    snapshot(
            get_conn_callback,
            f'{database}.{schema}',
            'tmp_in',
            f'{database}.{schema}.tmp_in',
            f'{database}.{schema}.tmp_scd',
            f'{database}.{schema}.tmp_batch_metadata',
            'txy',
            'xy',
            't',
            '20210614T050000',
            partitions=({'x': 1, 'y': 2},)
    )
    # a second round to stage the second set of partitions for a batch dated Jun 9; however, for one of the partitions in it a newer batch exists:
    is_protection_okay = False
    try:  
        snapshot(
                get_conn_callback,
                f'{database}.{schema}',
                'tmp_in',
                f'{database}.{schema}.tmp_in',
                f'{database}.{schema}.tmp_scd',
                f'{database}.{schema}.tmp_batch_metadata',
                'txy',
                'xy',
                't',
                '20210609T050000',
                partitions=({'x': 1, 'y': 2}, {'x': 3, 'y': 4})
        )
    except ValueError as e:
        print('--------------')
        print(e)
        print('--------------')
        if 'no earlier' in str(e):
            is_protection_okay = True
        else: 
            raise
    rows = run_and_fetchall_own_connection(get_conn_callback, f'SELECT * FROM {database}.{schema}.tmp_scd')
    drop_tables(get_conn_callback, database, schema)
    print(rows)
    is_one_row = (len(rows) == 1)
    is_most_recent_always_true = all([row['_scd_is_most_recent'] for row in rows])
    is_deleted_always_none = all([row['_scd_deleted_timestamp'] is None for row in rows])
    return _is_okay_invariants_scd_table(rows) and is_one_row and is_most_recent_always_true and is_deleted_always_none and is_protection_okay


def is_scenario_14_success(database, schema, get_conn_callback) -> bool:
    create_tables(get_conn_callback, database, schema, 't', {'x': 'INT', 'y': 'INT'})
    run_own_connection(get_conn_callback, f"INSERT INTO {database}.{schema}.tmp_in (t, x, y) VALUES ('20210609T050000', 1, 2), ('20210609T050000', 2, 1);")
    snapshot(
            get_conn_callback,
            f'{database}.{schema}',
            'tmp_in',
            f'{database}.{schema}.tmp_in',
            f'{database}.{schema}.tmp_scd',
            f'{database}.{schema}.tmp_batch_metadata',
            'txy',
            'xy',
            't',
            '20210609T050000',
            batch_metadata={'source': 'ONE_TIME_DUMP'}
    )
    rows = run_and_fetchall_own_connection(get_conn_callback, f'SELECT * FROM {database}.{schema}.tmp_scd')
    drop_tables(get_conn_callback, database, schema)
    print(rows)
    is_two_rows = (len(rows) == 2)
    is_most_recent_always_true = all([row['_scd_is_most_recent'] for row in rows])
    return _is_okay_invariants_scd_table(rows) and is_two_rows and is_most_recent_always_true 


def is_scenario_15_success(database, schema, get_conn_callback) -> bool:
    create_tables(get_conn_callback, database, schema, 't', {'x': 'INT', 'y': 'INT'}, partition_columns=('x',))
    run_own_connection(get_conn_callback, f"INSERT INTO {database}.{schema}.tmp_in (t, x, y) VALUES ('20210609T050000', 1, 1), ('20210609T050000', 1, 2), ('20210609T050000', 2, 1);")
    # use partitions with batch metadata, this time multiple partitions
    snapshot(
            get_conn_callback,
            f'{database}.{schema}',
            'tmp_in',
            f'{database}.{schema}.tmp_in',
            f'{database}.{schema}.tmp_scd',
            f'{database}.{schema}.tmp_batch_metadata',
            'txy',
            'xy',
            't',
            '20210609T050000',
            partitions=({'x': 1}, {'x': 2}),
            batch_metadata={'source': 'NEW_YORK_TIMES'},
            run_id='my_test_run'
    )
    rows = run_and_fetchall_own_connection(get_conn_callback, f'SELECT * FROM {database}.{schema}.tmp_scd')
    drop_tables(get_conn_callback, database, schema)
    print(rows)
    is_three_rows = (len(rows) == 3)
    is_most_recent_always_true = all([row['_scd_is_most_recent'] for row in rows])
    return _is_okay_invariants_scd_table(rows) and is_three_rows and is_most_recent_always_true


def is_scenario_16_success(database, schema, get_conn_callback) -> bool:
    create_tables(get_conn_callback, database, schema, 't', {'x': 'INT', 'y': 'INT'}, partition_columns=('x', 'y'))
    run_own_connection(get_conn_callback, f"INSERT INTO {database}.{schema}.tmp_in (t, x, y) VALUES ('20210609T050000', 1, 1), ('20210609T050000', 1, 2), ('20210609T050000', 2, 1);")
    # use partitions, two partition columns this time
    snapshot(
            get_conn_callback,
            f'{database}.{schema}',
            'tmp_in',
            f'{database}.{schema}.tmp_in',
            f'{database}.{schema}.tmp_scd',
            f'{database}.{schema}.tmp_batch_metadata',
            'txy',
            'xy',
            't',
            '20210609T050000',
            partitions=({'x': 1, 'y': 2},),
            batch_metadata={'source': 'WEEKLY'}
    )
    rows = run_and_fetchall_own_connection(get_conn_callback, f'SELECT * FROM {database}.{schema}.tmp_scd')
    drop_tables(get_conn_callback, database, schema)
    print(rows)
    is_one_row = (len(rows) == 1)
    is_most_recent_always_true = all([row['_scd_is_most_recent'] for row in rows])
    return _is_okay_invariants_scd_table(rows) and is_one_row and is_most_recent_always_true


def is_scenario_17_success(database, schema, get_conn_callback) -> bool:
    create_tables(get_conn_callback, database, schema, 't_', {'x': 'INT', 'y': 'INT'}, partition_columns=('x', 'y'))
    run_own_connection(get_conn_callback, f"INSERT INTO {database}.{schema}.tmp_in (t_, x, y) VALUES ('20210609T050000', 1, 1), ('20210609T050000', 1, 2), ('20210609T050000', 2, 1);")
    # Use lists or tuples (like we're supposed to) instead of strings that Python gladly iterates over (Python is a toy language)
    # Also use a multi letter ts_nodash_col to surface a silly bug that existed once in snapshot
    snapshot(
            get_conn_callback,
            f'{database}.{schema}',
            'tmp_in',
            f'{database}.{schema}.tmp_in',
            f'{database}.{schema}.tmp_scd',
            f'{database}.{schema}.tmp_batch_metadata',
            ('t_', 'x', 'y'),
            ('x', 'y'),
            't_',
            '20210609T050000',
            partitions=({'x': 1, 'y': 2},),
            batch_metadata={'source': 'WEEKLY'}
    )
    rows = run_and_fetchall_own_connection(get_conn_callback, f'SELECT * FROM {database}.{schema}.tmp_scd')
    drop_tables(get_conn_callback, database, schema)
    print(rows)
    is_one_row = (len(rows) == 1)
    is_most_recent_always_true = all([row['_scd_is_most_recent'] for row in rows])
    return _is_okay_invariants_scd_table(rows) and is_one_row and is_most_recent_always_true
    

def is_scenario_18_success(database, schema, get_conn_callback) -> bool:
    # use a NOT NULL constraint in one of the columns to surface a bug we had in inserting the deletions in the changes table
    create_tables(get_conn_callback, database, schema, 't', {'x': 'INT NOT NULL', 'y': 'INT'})
    # introduce a deletion in the second batch (remember, each batch is considered to be a full load)
    run_own_connection(get_conn_callback, f"INSERT INTO {database}.{schema}.tmp_in (t, x, y) VALUES ('20210609T050000', 1, 2), ('20210609T050000', 2, 1), "
              "('20210614T050000', 1, 1);")
    snapshot(
            get_conn_callback,
            f'{database}.{schema}',
            'tmp_in',
            f'{database}.{schema}.tmp_in',
            f'{database}.{schema}.tmp_scd',
            f'{database}.{schema}.tmp_batch_metadata',
            'txy',
            'x',
            't',
            '20210609T050000'
    )
    # a second round to stage the second batch 
    snapshot(
            get_conn_callback,
            f'{database}.{schema}',
            'tmp_in',
            f'{database}.{schema}.tmp_in',
            f'{database}.{schema}.tmp_scd',
            f'{database}.{schema}.tmp_batch_metadata',
            'txy',
            'x',
            't',
            '20210614T050000'
    )
    rows = run_and_fetchall_own_connection(get_conn_callback, f'SELECT * FROM {database}.{schema}.tmp_scd')
    drop_tables(get_conn_callback, database, schema)
    print(rows)
    is_three_rows = (len(rows) == 3)
    is_most_recent_not_always_true = not all([row['_scd_is_most_recent'] for row in rows])
    is_a_deleted_row_present = any([row['_scd_deleted_timestamp'] for row in rows])
    return _is_okay_invariants_scd_table(rows) and is_three_rows and is_most_recent_not_always_true and is_a_deleted_row_present


def is_scenario_19_success(database, schema, get_conn_callback) -> bool:
    create_tables(get_conn_callback, database, schema, 't', {'x': 'INT', 'y': 'INT'}, partition_columns=('x',))
    # Check that a new row is only inserted for the partition it belongs to
    run_own_connection(get_conn_callback, f"INSERT INTO {database}.{schema}.tmp_in (t, x, y) VALUES ('20210609T050000', 1, 1), ('20210614T050000', 2, 2), ('20210629T050000', 2, 2), ('20210629T050000', 2, 3);")
    snapshot(
            get_conn_callback,
            f'{database}.{schema}',
            'tmp_in',
            f'{database}.{schema}.tmp_in',
            f'{database}.{schema}.tmp_scd',
            f'{database}.{schema}.tmp_batch_metadata',
            'txy',
            'xy',
            't',
            '20210609T050000',
            partitions=({'x': 1},)
    )
    # a second round to stage the second batch 
    snapshot(
            get_conn_callback,
            f'{database}.{schema}',
            'tmp_in',
            f'{database}.{schema}.tmp_in',
            f'{database}.{schema}.tmp_scd',
            f'{database}.{schema}.tmp_batch_metadata',
            'txy',
            'xy',
            't',
            '20210614T050000',
            partitions=({'x': 2},)
    )
    # a third round to stage the third batch 
    snapshot(
            get_conn_callback,
            f'{database}.{schema}',
            'tmp_in',
            f'{database}.{schema}.tmp_in',
            f'{database}.{schema}.tmp_scd',
            f'{database}.{schema}.tmp_batch_metadata',
            'txy',
            'xy',
            't',
            '20210629T050000',
            partitions=({'x': 2},)
    )
    rows = run_and_fetchall_own_connection(get_conn_callback, f'SELECT * FROM {database}.{schema}.tmp_scd')
    drop_tables(get_conn_callback, database, schema)
    print(rows)
    is_three_rows = (len(rows) == 3)
    is_most_recent_always_true = all([row['_scd_is_most_recent'] for row in rows])
    is_deleted_always_none = all([row['_scd_deleted_timestamp'] is None for row in rows])
    return _is_okay_invariants_scd_table(rows) and is_three_rows and is_most_recent_always_true and is_deleted_always_none


def is_scenario_20_success(database, schema, get_conn_callback) -> bool:
    create_tables(get_conn_callback, database, schema, 't', {'x': 'INT', 'y': 'INT'}, partition_columns=('x',))
    # Check that a new but unchanged row does not cause an insert of the new row + an invalidation of the old row
    run_own_connection(get_conn_callback, f"INSERT INTO {database}.{schema}.tmp_in (t, x, y) VALUES ('20210614T050000', 2, 2), ('20210629T050000', 2, 2);")
    snapshot(
            get_conn_callback,
            f'{database}.{schema}',
            'tmp_in',
            f'{database}.{schema}.tmp_in',
            f'{database}.{schema}.tmp_scd',
            f'{database}.{schema}.tmp_batch_metadata',
            'txy',
            'xy',
            't',
            '20210614T050000',
            partitions=({'x': 2},)
    )
    # a second round to stage the second batch 
    snapshot(
            get_conn_callback,
            f'{database}.{schema}',
            'tmp_in',
            f'{database}.{schema}.tmp_in',
            f'{database}.{schema}.tmp_scd',
            f'{database}.{schema}.tmp_batch_metadata',
            'txy',
            'xy',
            't',
            '20210629T050000',
            partitions=({'x': 2},)
    )
    rows = run_and_fetchall_own_connection(get_conn_callback, f'SELECT * FROM {database}.{schema}.tmp_scd')
    drop_tables(get_conn_callback, database, schema)
    print(rows)
    is_one_row = (len(rows) == 1)
    is_most_recent_always_true = all([row['_scd_is_most_recent'] for row in rows])
    is_deleted_always_none = all([row['_scd_deleted_timestamp'] is None for row in rows])
    return _is_okay_invariants_scd_table(rows) and is_one_row and is_most_recent_always_true and is_deleted_always_none


# ========================================== #
# Run all scenario's also without zombie check
# ========================================== #
def is_scenario_21_success(database, schema, get_conn_callback) -> bool:
    create_tables(get_conn_callback, database, schema, 't', {'x': 'INT', 'y': 'INT'})
    run_own_connection(get_conn_callback, f"INSERT INTO {database}.{schema}.tmp_in (t, x, y) VALUES ('20210609T050000', 1, 2), ('20210609T050000', 2, 1);")
    snapshot(
            get_conn_callback,
            f'{database}.{schema}',
            'tmp_in',
            f'{database}.{schema}.tmp_in',
            f'{database}.{schema}.tmp_scd',
            f'{database}.{schema}.tmp_batch_metadata',
            'txy',
            'xy',
            't',
            '20210609T050000',
            do_zombie_check=False
    )
    rows = run_and_fetchall_own_connection(get_conn_callback, f'SELECT * FROM {database}.{schema}.tmp_scd')
    drop_tables(get_conn_callback, database, schema)
    print(rows)
    is_two_rows = (len(rows) == 2)
    is_most_recent_always_true = all([row['_scd_is_most_recent'] for row in rows])
    return _is_okay_invariants_scd_table(rows, False) and is_two_rows and is_most_recent_always_true 


def is_scenario_22_success(database, schema, get_conn_callback) -> bool:
    create_tables(get_conn_callback, database, schema, 't', {'x': 'INT', 'y': 'INT'})
    # introduce a duplicate row in the source table; only during the second step, where the changes are staged, will it be deduplicated.
    run_own_connection(get_conn_callback, f"INSERT INTO {database}.{schema}.tmp_in (t, x, y) VALUES ('20210609T050000', 1, 2), ('20210609T050000', 2, 1), ('20210609T050000', 2, 1);")
    snapshot(
            get_conn_callback,
            f'{database}.{schema}',
            'tmp_in',
            f'{database}.{schema}.tmp_in',
            f'{database}.{schema}.tmp_scd',
            f'{database}.{schema}.tmp_batch_metadata',
            'txy',
            'xy',
            't',
            '20210609T050000',
            fail_on_duplicates=False,
            do_zombie_check=False
    )
    rows = run_and_fetchall_own_connection(get_conn_callback, f'SELECT * FROM {database}.{schema}.tmp_scd')
    drop_tables(get_conn_callback, database, schema)
    print(rows)
    is_two_rows = (len(rows) == 2)
    is_most_recent_always_true = all([row['_scd_is_most_recent'] for row in rows])
    return _is_okay_invariants_scd_table(rows, False) and is_two_rows and is_most_recent_always_true


def is_scenario_23_success(database, schema, get_conn_callback) -> bool:
    create_tables(get_conn_callback, database, schema, 't', {'x': 'INT', 'y': 'INT'}, partition_columns=('x',))
    run_own_connection(get_conn_callback, f"INSERT INTO {database}.{schema}.tmp_in (t, x, y) VALUES ('20210609T050000', 1, 1), ('20210609T050000', 1, 2), ('20210609T050000', 2, 1);")
    # use partitions
    snapshot(
            get_conn_callback,
            f'{database}.{schema}',
            'tmp_in',
            f'{database}.{schema}.tmp_in',
            f'{database}.{schema}.tmp_scd',
            f'{database}.{schema}.tmp_batch_metadata',
            'txy',
            'xy',
            't',
            '20210609T050000',
            partitions=({'x': 1},),
            do_zombie_check=False
    )
    rows = run_and_fetchall_own_connection(get_conn_callback, f'SELECT * FROM {database}.{schema}.tmp_scd')
    drop_tables(get_conn_callback, database, schema)
    print(rows)
    is_two_rows = (len(rows) == 2)
    is_most_recent_always_true = all([row['_scd_is_most_recent'] for row in rows])
    return _is_okay_invariants_scd_table(rows, False) and is_two_rows and is_most_recent_always_true



def is_scenario_24_success(database, schema, get_conn_callback) -> bool:
    create_tables(get_conn_callback, database, schema, 't', {'x': 'INT', 'y': 'INT'})
    run_own_connection(get_conn_callback, f"INSERT INTO {database}.{schema}.tmp_in (t, x, y) VALUES ('20210609T050000', 1, 2), ('20210609T050000', 2, 1);")
    # use a column with values (y) that is not part of a unique key
    snapshot(
            get_conn_callback,
            f'{database}.{schema}',
            'tmp_in',
            f'{database}.{schema}.tmp_in',
            f'{database}.{schema}.tmp_scd',
            f'{database}.{schema}.tmp_batch_metadata',
            'txy',
            'x',
            't',
            '20210609T050000',
            do_zombie_check=False
    )
    rows = run_and_fetchall_own_connection(get_conn_callback, f'SELECT * FROM {database}.{schema}.tmp_scd')
    drop_tables(get_conn_callback, database, schema)
    print(rows)
    is_two_rows = (len(rows) == 2)
    is_most_recent_always_true = all([row['_scd_is_most_recent'] for row in rows])
    return _is_okay_invariants_scd_table(rows, False) and is_two_rows and is_most_recent_always_true


def is_scenario_25_success(database, schema, get_conn_callback) -> bool:
    create_tables(get_conn_callback, database, schema, 't', {'x': 'INT', 'y': 'INT'})
    # now in this table there are two batches worth of data
    run_own_connection(get_conn_callback, f"INSERT INTO {database}.{schema}.tmp_in (t, x, y) VALUES ('20210609T050000', 1, 2), ('20210609T050000', 2, 1), "
                                          "('20210614T050000', 1, 1), ('20210614T050000', 2, 1);")
    snapshot(
            get_conn_callback,
            f'{database}.{schema}',
            'tmp_in',
            f'{database}.{schema}.tmp_in',
            f'{database}.{schema}.tmp_scd',
            f'{database}.{schema}.tmp_batch_metadata',
            'txy',
            'x',
            't',
            '20210609T050000',
            do_zombie_check=False
    )
    # a second round to stage the second batch 
    snapshot(
            get_conn_callback,
            f'{database}.{schema}',
            'tmp_in',
            f'{database}.{schema}.tmp_in',
            f'{database}.{schema}.tmp_scd',
            f'{database}.{schema}.tmp_batch_metadata',
            'txy',
            'x',
            't',
            '20210614T050000',
            do_zombie_check=False
    )
    rows = run_and_fetchall_own_connection(get_conn_callback, f'SELECT * FROM {database}.{schema}.tmp_scd')
    drop_tables(get_conn_callback, database, schema)
    print(rows)
    is_three_rows = (len(rows) == 3)
    is_most_recent_not_always_true = not all([row['_scd_is_most_recent'] for row in rows])
    return _is_okay_invariants_scd_table(rows, False) and is_three_rows and is_most_recent_not_always_true


def is_scenario_26_success(database, schema, get_conn_callback) -> bool:
    create_tables(get_conn_callback, database, schema, 't', {'x': 'INT', 'y': 'INT'})
    # introduce a deletion in the second batch (remember, each batch is considered to be a full load)
    run_own_connection(get_conn_callback, f"INSERT INTO {database}.{schema}.tmp_in (t, x, y) VALUES ('20210609T050000', 1, 2), ('20210609T050000', 2, 1), "
              "('20210614T050000', 1, 1);")
    snapshot(
            get_conn_callback,
            f'{database}.{schema}',
            'tmp_in',
            f'{database}.{schema}.tmp_in',
            f'{database}.{schema}.tmp_scd',
            f'{database}.{schema}.tmp_batch_metadata',
            'txy',
            'x',
            't',
            '20210609T050000',
            do_zombie_check=False
    )
    # a second round to stage the second batch 
    snapshot(
            get_conn_callback,
            f'{database}.{schema}',
            'tmp_in',
            f'{database}.{schema}.tmp_in',
            f'{database}.{schema}.tmp_scd',
            f'{database}.{schema}.tmp_batch_metadata',
            'txy',
            'x',
            't',
            '20210614T050000',
            do_zombie_check=False
    )
    rows = run_and_fetchall_own_connection(get_conn_callback, f'SELECT * FROM {database}.{schema}.tmp_scd')
    drop_tables(get_conn_callback, database, schema)
    print(rows)
    is_three_rows = (len(rows) == 3)
    is_most_recent_not_always_true = not all([row['_scd_is_most_recent'] for row in rows])
    is_a_deleted_row_present = any([row['_scd_deleted_timestamp'] for row in rows])
    return _is_okay_invariants_scd_table(rows, False) and is_three_rows and is_most_recent_not_always_true and is_a_deleted_row_present


def is_scenario_27_success(database, schema, get_conn_callback) -> bool:
    create_tables(get_conn_callback, database, schema, 't', {'x': 'INT', 'y': 'INT'})
    # introduce a deletion in the second batch (remember, each batch is considered to be a full load)
    # then, in the third batch, re-introduce a previously deleted row, without altering any of the values
    # In other words, introduce a zombie, but without doing a zombie check. The result will be a duplicate valid row for an SCD
    # I wonder if have an invariant check already that returns False
    run_own_connection(get_conn_callback, f"INSERT INTO {database}.{schema}.tmp_in (t, x, y) VALUES ('20210609T050000', 1, 2), ('20210609T050000', 2, 1), "
              "('20210614T050000', 1, 1), "
              "('20210619T050000', 1, 1), ('20210619T050000', 2, 1);")
    snapshot(
            get_conn_callback,
            f'{database}.{schema}',
            'tmp_in',
            f'{database}.{schema}.tmp_in',
            f'{database}.{schema}.tmp_scd',
            f'{database}.{schema}.tmp_batch_metadata',
            'txy',
            'x',
            't',
            '20210609T050000',
            do_zombie_check=False
    )
    # a second round to stage the second batch 
    snapshot(
            get_conn_callback,
            f'{database}.{schema}',
            'tmp_in',
            f'{database}.{schema}.tmp_in',
            f'{database}.{schema}.tmp_scd',
            f'{database}.{schema}.tmp_batch_metadata',
            'txy',
            'x',
            't',
            '20210614T050000',
            do_zombie_check=False
    )
    # a third round to stage the third batch
    snapshot(
            get_conn_callback,
            f'{database}.{schema}',
            'tmp_in',
            f'{database}.{schema}.tmp_in',
            f'{database}.{schema}.tmp_scd',
            f'{database}.{schema}.tmp_batch_metadata',
            'txy',
            'x',
            't',
            '20210619T050000',
            do_zombie_check=False
    )
    rows = run_and_fetchall_own_connection(get_conn_callback, f'SELECT * FROM {database}.{schema}.tmp_scd')
    drop_tables(get_conn_callback, database, schema)
    print(rows)
    is_four_rows = (len(rows) == 4)
    is_most_recent_not_always_true = not all([row['_scd_is_most_recent'] for row in rows])
    is_a_deleted_row_present = any([row['_scd_deleted_timestamp'] for row in rows])
    return _is_okay_invariants_scd_table(rows, False) and is_four_rows and is_most_recent_not_always_true and is_a_deleted_row_present



def is_scenario_28_success(database, schema, get_conn_callback) -> bool:
    create_tables(get_conn_callback, database, schema, 't', {'x': 'INT', 'y': 'INT'})
    # introduce NULL values in the values (y) and make sure it is not seen as an update (NULL = NULL evaluates to NULL in SQL)
    run_own_connection(get_conn_callback, f"INSERT INTO {database}.{schema}.tmp_in (t, x, y) VALUES ('20210609T050000', 1, NULL), ('20210614T050000', 1, NULL);")
    snapshot(
            get_conn_callback,
            f'{database}.{schema}',
            'tmp_in',
            f'{database}.{schema}.tmp_in',
            f'{database}.{schema}.tmp_scd',
            f'{database}.{schema}.tmp_batch_metadata',
            'txy',
            'x',
            't',
            '20210609T050000',
            do_zombie_check=False
    )
    # a second round to stage the second batch 
    snapshot(
            get_conn_callback,
            f'{database}.{schema}',
            'tmp_in',
            f'{database}.{schema}.tmp_in',
            f'{database}.{schema}.tmp_scd',
            f'{database}.{schema}.tmp_batch_metadata',
            'txy',
            'x',
            't',
            '20210614T050000',
            do_zombie_check=False
    )
    rows = run_and_fetchall_own_connection(get_conn_callback, f'SELECT * FROM {database}.{schema}.tmp_scd')
    drop_tables(get_conn_callback, database, schema)
    print(rows)
    is_one_row = (len(rows) == 1)
    is_most_recent_always_true = all([row['_scd_is_most_recent'] for row in rows])
    return _is_okay_invariants_scd_table(rows, False) and is_one_row and is_most_recent_always_true


def is_scenario_29_success(database, schema, get_conn_callback) -> bool:
    create_tables(get_conn_callback, database, schema, 't', {'x': 'INT', 'y': 'INT'})
    # introduce NULL values in the unique compound key (x) and make sure it is treated as an actual value
    # (Sadly, in real life tables sometimes have no unique compound key, and we have to include columns that can be null in a "guessed" unique compound key in order to make 
    # rows unique in data observed so far.)
    run_own_connection(get_conn_callback, f"INSERT INTO {database}.{schema}.tmp_in (t, x, y) VALUES ('20210609T050000', NULL, 1);")
    snapshot(
            get_conn_callback,
            f'{database}.{schema}',
            'tmp_in',
            f'{database}.{schema}.tmp_in',
            f'{database}.{schema}.tmp_scd',
            f'{database}.{schema}.tmp_batch_metadata',
            'txy',
            'x',
            't',
            '20210609T050000',
            do_zombie_check=False
    )
    rows = run_and_fetchall_own_connection(get_conn_callback, f'SELECT * FROM {database}.{schema}.tmp_scd')
    drop_tables(get_conn_callback, database, schema)
    print(rows)
    is_one_row = (len(rows) == 1)
    is_most_recent_always_true = all([row['_scd_is_most_recent'] for row in rows])
    return _is_okay_invariants_scd_table(rows, False) and is_one_row and is_most_recent_always_true


def is_scenario_30_success(database, schema, get_conn_callback) -> bool:
    create_tables(get_conn_callback, database, schema, 't', {'x': 'INT', 'y': 'INT'})
    # introduce NULL values in the unique compound key (x) and make sure it is treated as an actual value
    # (Sadly, in real life tables sometimes have no unique compound key, and we have to include columns that can be null in a "guessed" unique compound key in order to make 
    # rows unique in data observed so far.)
    # Also, add multiple rows with the same NULL compound key, and check that they are deduplicated as a single _scd_normalised_key (with an arbitrary row selected)
    run_own_connection(get_conn_callback, f"INSERT INTO {database}.{schema}.tmp_in (t, x, y) VALUES ('20210609T050000', NULL, 1), ('20210609T050000', NULL, 2);")
    snapshot(
            get_conn_callback,
            f'{database}.{schema}',
            'tmp_in',
            f'{database}.{schema}.tmp_in',
            f'{database}.{schema}.tmp_scd',
            f'{database}.{schema}.tmp_batch_metadata',
            'txy',
            'x',
            't',
            '20210609T050000',
            fail_on_duplicates=False,
            do_zombie_check=False
    )
    rows = run_and_fetchall_own_connection(get_conn_callback, f'SELECT * FROM {database}.{schema}.tmp_scd')
    drop_tables(get_conn_callback, database, schema)
    print(rows)
    is_one_row = (len(rows) == 1)
    is_most_recent_always_true = all([row['_scd_is_most_recent'] for row in rows])
    return _is_okay_invariants_scd_table(rows, False) and is_one_row and is_most_recent_always_true
    

def is_scenario_31_success(database, schema, get_conn_callback) -> bool:
    create_tables(get_conn_callback, database, schema, 't', {'x': 'INT', 'y': 'INT'})
    # introduce NULL values in the unique compound key (x) and make sure it is treated as an actual value
    # (Sadly, in real life tables sometimes have no unique compound key, and we have to include columns that can be null in a "guessed" unique compound key in order to make 
    # rows unique in data observed so far.)
    # Also, check if we can update rows with a NULL value in a column in the compound key
    run_own_connection(get_conn_callback, f"INSERT INTO {database}.{schema}.tmp_in (t, x, y) VALUES ('20210609T050000', NULL, 1), ('20210614T050000', NULL, 2);")
    snapshot(
            get_conn_callback,
            f'{database}.{schema}',
            'tmp_in',
            f'{database}.{schema}.tmp_in',
            f'{database}.{schema}.tmp_scd',
            f'{database}.{schema}.tmp_batch_metadata',
            'txy',
            'x',
            't',
            '20210609T050000',
            do_zombie_check=False
    )
    # a second round to stage the second batch 
    snapshot(
            get_conn_callback,
            f'{database}.{schema}',
            'tmp_in',
            f'{database}.{schema}.tmp_in',
            f'{database}.{schema}.tmp_scd',
            f'{database}.{schema}.tmp_batch_metadata',
            'txy',
            'x',
            't',
            '20210614T050000',
            do_zombie_check=False
    )
    rows = run_and_fetchall_own_connection(get_conn_callback, f'SELECT * FROM {database}.{schema}.tmp_scd')
    drop_tables(get_conn_callback, database, schema)
    print(rows)
    is_two_rows = (len(rows) == 2)
    is_most_recent_not_always_true = not all([row['_scd_is_most_recent'] for row in rows])
    return _is_okay_invariants_scd_table(rows, False) and is_two_rows and is_most_recent_not_always_true


def is_scenario_32_success(database, schema, get_conn_callback) -> bool:
    create_tables(get_conn_callback, database, schema, 't', {'x': 'INT', 'y': 'INT'}, partition_columns=('x',))
    # Check that partitions are handled okay, and that, if they are used, only rows in scd_table from those partitions will ever be marked for deletion
    run_own_connection(get_conn_callback, f"INSERT INTO {database}.{schema}.tmp_in (t, x, y) VALUES ('20210609T050000', 1, 1), ('20210614T050000', 2, 2);")
    snapshot(
            get_conn_callback,
            f'{database}.{schema}',
            'tmp_in',
            f'{database}.{schema}.tmp_in',
            f'{database}.{schema}.tmp_scd',
            f'{database}.{schema}.tmp_batch_metadata',
            'txy',
            'xy',
            't',
            '20210609T050000',
            partitions=({'x': 1},),
            do_zombie_check=False
    )
    # a second round to stage the second batch 
    snapshot(
            get_conn_callback,
            f'{database}.{schema}',
            'tmp_in',
            f'{database}.{schema}.tmp_in',
            f'{database}.{schema}.tmp_scd',
            f'{database}.{schema}.tmp_batch_metadata',
            'txy',
            'xy',
            't',
            '20210614T050000',
            partitions=({'x': 2},),
            do_zombie_check=False
    )
    rows = run_and_fetchall_own_connection(get_conn_callback, f'SELECT * FROM {database}.{schema}.tmp_scd')
    drop_tables(get_conn_callback, database, schema)
    print(rows)
    is_two_rows = (len(rows) == 2)
    is_most_recent_always_true = all([row['_scd_is_most_recent'] for row in rows])
    is_deleted_always_none = all([row['_scd_deleted_timestamp'] is None for row in rows])
    return _is_okay_invariants_scd_table(rows, False) and is_two_rows and is_most_recent_always_true and is_deleted_always_none


def is_scenario_33_success(database, schema, get_conn_callback) -> bool:
    create_tables(get_conn_callback, database, schema, 't', {'x': 'INT', 'y': 'INT'}, partition_columns=('x', 'y'))
    run_own_connection(get_conn_callback, f"INSERT INTO {database}.{schema}.tmp_in (t, x, y) VALUES ('20210609T050000', 1, 2), ('20210609T050000', 3, 4), ('20210614T050000', 1, 2);")
    snapshot(
            get_conn_callback,
            f'{database}.{schema}',
            'tmp_in',
            f'{database}.{schema}.tmp_in',
            f'{database}.{schema}.tmp_scd',
            f'{database}.{schema}.tmp_batch_metadata',
            'txy',
            'xy',
            't',
            '20210614T050000',
            partitions=({'x': 1, 'y': 2},),
            do_zombie_check=False
    )
    # a second round to stage the second set of partitions for a batch dated Jun 9; however, for one of the partitions in it a newer batch exists:
    is_protection_okay = False
    try:  
        snapshot(
                get_conn_callback,
                f'{database}.{schema}',
                'tmp_in',
                f'{database}.{schema}.tmp_in',
                f'{database}.{schema}.tmp_scd',
                f'{database}.{schema}.tmp_batch_metadata',
                'txy',
                'xy',
                't',
                '20210609T050000',
                partitions=({'x': 1, 'y': 2}, {'x': 3, 'y': 4}),
                do_zombie_check=False
        )
    except ValueError as e:
        print('--------------')
        print(e)
        print('--------------')
        if 'no earlier' in str(e):
            is_protection_okay = True
        else: 
            raise
    rows = run_and_fetchall_own_connection(get_conn_callback, f'SELECT * FROM {database}.{schema}.tmp_scd')
    drop_tables(get_conn_callback, database, schema)
    print(rows)
    is_one_row = (len(rows) == 1)
    is_most_recent_always_true = all([row['_scd_is_most_recent'] for row in rows])
    is_deleted_always_none = all([row['_scd_deleted_timestamp'] is None for row in rows])
    return _is_okay_invariants_scd_table(rows, False) and is_one_row and is_most_recent_always_true and is_deleted_always_none and is_protection_okay


def is_scenario_34_success(database, schema, get_conn_callback) -> bool:
    create_tables(get_conn_callback, database, schema, 't', {'x': 'INT', 'y': 'INT'})
    run_own_connection(get_conn_callback, f"INSERT INTO {database}.{schema}.tmp_in (t, x, y) VALUES ('20210609T050000', 1, 2), ('20210609T050000', 2, 1);")
    snapshot(
            get_conn_callback,
            f'{database}.{schema}',
            'tmp_in',
            f'{database}.{schema}.tmp_in',
            f'{database}.{schema}.tmp_scd',
            f'{database}.{schema}.tmp_batch_metadata',
            'txy',
            'xy',
            't',
            '20210609T050000',
            batch_metadata={'source': 'ONE_TIME_DUMP'},
            do_zombie_check=False
    )
    rows = run_and_fetchall_own_connection(get_conn_callback, f'SELECT * FROM {database}.{schema}.tmp_scd')
    drop_tables(get_conn_callback, database, schema)
    print(rows)
    is_two_rows = (len(rows) == 2)
    is_most_recent_always_true = all([row['_scd_is_most_recent'] for row in rows])
    return _is_okay_invariants_scd_table(rows, False) and is_two_rows and is_most_recent_always_true 


def is_scenario_35_success(database, schema, get_conn_callback) -> bool:
    create_tables(get_conn_callback, database, schema, 't', {'x': 'INT', 'y': 'INT'}, partition_columns=('x',))
    run_own_connection(get_conn_callback, f"INSERT INTO {database}.{schema}.tmp_in (t, x, y) VALUES ('20210609T050000', 1, 1), ('20210609T050000', 1, 2), ('20210609T050000', 2, 1);")
    # use partitions with batch metadata, this time multiple partitions
    snapshot(
            get_conn_callback,
            f'{database}.{schema}',
            'tmp_in',
            f'{database}.{schema}.tmp_in',
            f'{database}.{schema}.tmp_scd',
            f'{database}.{schema}.tmp_batch_metadata',
            'txy',
            'xy',
            't',
            '20210609T050000',
            partitions=({'x': 1}, {'x': 2}),
            batch_metadata={'source': 'NEW_YORK_TIMES'},
            run_id='my_test_run',
            do_zombie_check=False
    )
    rows = run_and_fetchall_own_connection(get_conn_callback, f'SELECT * FROM {database}.{schema}.tmp_scd')
    drop_tables(get_conn_callback, database, schema)
    print(rows)
    is_three_rows = (len(rows) == 3)
    is_most_recent_always_true = all([row['_scd_is_most_recent'] for row in rows])
    return _is_okay_invariants_scd_table(rows, False) and is_three_rows and is_most_recent_always_true


def is_scenario_36_success(database, schema, get_conn_callback) -> bool:
    create_tables(get_conn_callback, database, schema, 't', {'x': 'INT', 'y': 'INT'}, partition_columns=('x', 'y'))
    run_own_connection(get_conn_callback, f"INSERT INTO {database}.{schema}.tmp_in (t, x, y) VALUES ('20210609T050000', 1, 1), ('20210609T050000', 1, 2), ('20210609T050000', 2, 1);")
    # use partitions, two partition columns this time
    snapshot(
            get_conn_callback,
            f'{database}.{schema}',
            'tmp_in',
            f'{database}.{schema}.tmp_in',
            f'{database}.{schema}.tmp_scd',
            f'{database}.{schema}.tmp_batch_metadata',
            'txy',
            'xy',
            't',
            '20210609T050000',
            partitions=({'x': 1, 'y': 2},),
            batch_metadata={'source': 'WEEKLY'},
            do_zombie_check=False
    )
    rows = run_and_fetchall_own_connection(get_conn_callback, f'SELECT * FROM {database}.{schema}.tmp_scd')
    drop_tables(get_conn_callback, database, schema)
    print(rows)
    is_one_row = (len(rows) == 1)
    is_most_recent_always_true = all([row['_scd_is_most_recent'] for row in rows])
    return _is_okay_invariants_scd_table(rows, False) and is_one_row and is_most_recent_always_true


def is_scenario_37_success(database, schema, get_conn_callback) -> bool:
    create_tables(get_conn_callback, database, schema, 't_', {'x': 'INT', 'y': 'INT'}, partition_columns=('x', 'y'))
    run_own_connection(get_conn_callback, f"INSERT INTO {database}.{schema}.tmp_in (t_, x, y) VALUES ('20210609T050000', 1, 1), ('20210609T050000', 1, 2), ('20210609T050000', 2, 1);")
    # Use lists or tuples (like we're supposed to) instead of strings that Python gladly iterates over (Python is a toy language)
    # Also use a multi letter ts_nodash_col to surface a silly bug that existed once in snapshot
    snapshot(
            get_conn_callback,
            f'{database}.{schema}',
            'tmp_in',
            f'{database}.{schema}.tmp_in',
            f'{database}.{schema}.tmp_scd',
            f'{database}.{schema}.tmp_batch_metadata',
            ('t_', 'x', 'y'),
            ('x', 'y'),
            't_',
            '20210609T050000',
            partitions=({'x': 1, 'y': 2},),
            batch_metadata={'source': 'WEEKLY'},
            do_zombie_check=False
    )
    rows = run_and_fetchall_own_connection(get_conn_callback, f'SELECT * FROM {database}.{schema}.tmp_scd')
    drop_tables(get_conn_callback, database, schema)
    print(rows)
    is_one_row = (len(rows) == 1)
    is_most_recent_always_true = all([row['_scd_is_most_recent'] for row in rows])
    return _is_okay_invariants_scd_table(rows, False) and is_one_row and is_most_recent_always_true
    

def is_scenario_38_success(database, schema, get_conn_callback) -> bool:
    # use a NOT NULL constraint in one of the columns to surface a bug we had in inserting the deletions in the changes table
    create_tables(get_conn_callback, database, schema, 't', {'x': 'INT NOT NULL', 'y': 'INT'})
    # introduce a deletion in the second batch (remember, each batch is considered to be a full load)
    run_own_connection(get_conn_callback, f"INSERT INTO {database}.{schema}.tmp_in (t, x, y) VALUES ('20210609T050000', 1, 2), ('20210609T050000', 2, 1), "
              "('20210614T050000', 1, 1);")
    snapshot(
            get_conn_callback,
            f'{database}.{schema}',
            'tmp_in',
            f'{database}.{schema}.tmp_in',
            f'{database}.{schema}.tmp_scd',
            f'{database}.{schema}.tmp_batch_metadata',
            'txy',
            'x',
            't',
            '20210609T050000',
            do_zombie_check=False
    )
    # a second round to stage the second batch 
    snapshot(
            get_conn_callback,
            f'{database}.{schema}',
            'tmp_in',
            f'{database}.{schema}.tmp_in',
            f'{database}.{schema}.tmp_scd',
            f'{database}.{schema}.tmp_batch_metadata',
            'txy',
            'x',
            't',
            '20210614T050000',
            do_zombie_check=False
    )
    rows = run_and_fetchall_own_connection(get_conn_callback, f'SELECT * FROM {database}.{schema}.tmp_scd')
    drop_tables(get_conn_callback, database, schema)
    print(rows)
    is_three_rows = (len(rows) == 3)
    is_most_recent_not_always_true = not all([row['_scd_is_most_recent'] for row in rows])
    is_a_deleted_row_present = any([row['_scd_deleted_timestamp'] for row in rows])
    return _is_okay_invariants_scd_table(rows, False) and is_three_rows and is_most_recent_not_always_true and is_a_deleted_row_present


def is_scenario_39_success(database, schema, get_conn_callback) -> bool:
    create_tables(get_conn_callback, database, schema, 't', {'x': 'INT', 'y': 'INT'}, partition_columns=('x',))
    # Check that a new row is only inserted for the partition it belongs to
    run_own_connection(get_conn_callback, f"INSERT INTO {database}.{schema}.tmp_in (t, x, y) VALUES ('20210609T050000', 1, 1), ('20210614T050000', 2, 2), ('20210629T050000', 2, 2), ('20210629T050000', 2, 3);")
    snapshot(
            get_conn_callback,
            f'{database}.{schema}',
            'tmp_in',
            f'{database}.{schema}.tmp_in',
            f'{database}.{schema}.tmp_scd',
            f'{database}.{schema}.tmp_batch_metadata',
            'txy',
            'xy',
            't',
            '20210609T050000',
            partitions=({'x': 1},),
            do_zombie_check=False
    )
    # a second round to stage the second batch 
    snapshot(
            get_conn_callback,
            f'{database}.{schema}',
            'tmp_in',
            f'{database}.{schema}.tmp_in',
            f'{database}.{schema}.tmp_scd',
            f'{database}.{schema}.tmp_batch_metadata',
            'txy',
            'xy',
            't',
            '20210614T050000',
            partitions=({'x': 2},),
            do_zombie_check=False
    )
    # a third round to stage the third batch 
    snapshot(
            get_conn_callback,
            f'{database}.{schema}',
            'tmp_in',
            f'{database}.{schema}.tmp_in',
            f'{database}.{schema}.tmp_scd',
            f'{database}.{schema}.tmp_batch_metadata',
            'txy',
            'xy',
            't',
            '20210629T050000',
            partitions=({'x': 2},),
            do_zombie_check=False
    )
    rows = run_and_fetchall_own_connection(get_conn_callback, f'SELECT * FROM {database}.{schema}.tmp_scd')
    drop_tables(get_conn_callback, database, schema)
    print(rows)
    is_three_rows = (len(rows) == 3)
    is_most_recent_always_true = all([row['_scd_is_most_recent'] for row in rows])
    is_deleted_always_none = all([row['_scd_deleted_timestamp'] is None for row in rows])
    return _is_okay_invariants_scd_table(rows, False) and is_three_rows and is_most_recent_always_true and is_deleted_always_none


def is_scenario_40_success(database, schema, get_conn_callback) -> bool:
    create_tables(get_conn_callback, database, schema, 't', {'x': 'INT', 'y': 'INT'}, partition_columns=('x',))
    # Check that a new but unchanged row does not cause an insert of the new row + an invalidation of the old row
    run_own_connection(get_conn_callback, f"INSERT INTO {database}.{schema}.tmp_in (t, x, y) VALUES ('20210614T050000', 2, 2), ('20210629T050000', 2, 2);")
    snapshot(
            get_conn_callback,
            f'{database}.{schema}',
            'tmp_in',
            f'{database}.{schema}.tmp_in',
            f'{database}.{schema}.tmp_scd',
            f'{database}.{schema}.tmp_batch_metadata',
            'txy',
            'xy',
            't',
            '20210614T050000',
            partitions=({'x': 2},),
            do_zombie_check=False
    )
    # a second round to stage the second batch 
    snapshot(
            get_conn_callback,
            f'{database}.{schema}',
            'tmp_in',
            f'{database}.{schema}.tmp_in',
            f'{database}.{schema}.tmp_scd',
            f'{database}.{schema}.tmp_batch_metadata',
            'txy',
            'xy',
            't',
            '20210629T050000',
            partitions=({'x': 2},),
            do_zombie_check=False
    )
    rows = run_and_fetchall_own_connection(get_conn_callback, f'SELECT * FROM {database}.{schema}.tmp_scd')
    drop_tables(get_conn_callback, database, schema)
    print(rows)
    is_one_row = (len(rows) == 1)
    is_most_recent_always_true = all([row['_scd_is_most_recent'] for row in rows])
    is_deleted_always_none = all([row['_scd_deleted_timestamp'] is None for row in rows])
    return _is_okay_invariants_scd_table(rows, False) and is_one_row and is_most_recent_always_true and is_deleted_always_none


# ========================================================== #
# End of running first 20 scenario's also without zombie check
# ========================================================== #


def is_scenario_41_success(database, schema, get_conn_callback) -> bool:
    create_tables(get_conn_callback, database, schema, 't', {'x': 'INT', 'y': 'INT'})
    run_own_connection(get_conn_callback, f"INSERT INTO {database}.{schema}.tmp_in (t, x, y) VALUES ('20210609T050000', 1, 2), ('20210609T050000', 2, 1);")
    snapshot(
            get_conn_callback,
            f'{database}.{schema}',
            'tmp_in',
            f'{database}.{schema}.tmp_in',
            f'{database}.{schema}.tmp_scd',
            f'{database}.{schema}.tmp_batch_metadata',
            'txy',
            'xy',
            't',
            '20210609T050000'
    )
    # A little mean. Doing a snapshot using a batch id for which there is no data in the source table at all.
    # Will it mark everything as deleted?
    snapshot(
            get_conn_callback,
            f'{database}.{schema}',
            'tmp_in',
            f'{database}.{schema}.tmp_in',
            f'{database}.{schema}.tmp_scd',
            f'{database}.{schema}.tmp_batch_metadata',
            'txy',
            'xy',
            't',
            '20210614T050000'
    )
    rows = run_and_fetchall_own_connection(get_conn_callback, f'SELECT * FROM {database}.{schema}.tmp_scd')
    drop_tables(get_conn_callback, database, schema)
    print(rows)
    is_two_rows = (len(rows) == 2)
    is_most_recent_always_true = all([row['_scd_is_most_recent'] for row in rows])
    is_deleted_always_true = all([row['_scd_deleted_timestamp'] is not None for row in rows])
    return _is_okay_invariants_scd_table(rows) and is_two_rows and is_most_recent_always_true and is_deleted_always_true


def is_scenario_42_success(database, schema, get_conn_callback) -> bool:
    create_tables(get_conn_callback, database, schema, 't', {'x': 'INT', 'y': 'INT'})
    run_own_connection(get_conn_callback, f"INSERT INTO {database}.{schema}.tmp_in (t, x, y) VALUES ('20210609T050000', 1, 2), ('20210609T050000', 2, 1);")
    snapshot(
            get_conn_callback,
            f'{database}.{schema}',
            'tmp_in',
            f'{database}.{schema}.tmp_in',
            f'{database}.{schema}.tmp_scd',
            f'{database}.{schema}.tmp_batch_metadata',
            'txy',
            'xy',
            't',
            '20210609T050000',
            do_zombie_check=False
    )
    # A little mean. Doing a snapshot using a batch id for which there is no data in the source table at all.
    # Will it mark everything as deleted?
    snapshot(
            get_conn_callback,
            f'{database}.{schema}',
            'tmp_in',
            f'{database}.{schema}.tmp_in',
            f'{database}.{schema}.tmp_scd',
            f'{database}.{schema}.tmp_batch_metadata',
            'txy',
            'xy',
            't',
            '20210614T050000',
            do_zombie_check=False
    )
    rows = run_and_fetchall_own_connection(get_conn_callback, f'SELECT * FROM {database}.{schema}.tmp_scd')
    drop_tables(get_conn_callback, database, schema)
    print(rows)
    is_two_rows = (len(rows) == 2)
    is_most_recent_always_true = all([row['_scd_is_most_recent'] for row in rows])
    is_deleted_always_true = all([row['_scd_deleted_timestamp'] is not None for row in rows])
    return _is_okay_invariants_scd_table(rows) and is_two_rows and is_most_recent_always_true and is_deleted_always_true


def is_scenario_43_success(database, schema, get_conn_callback) -> bool:
    create_tables(get_conn_callback, database, schema, 't', {'x': 'INT', 'y': 'INT'}, partition_columns=('x',))
    # In the second batch there is only data for a different partition from the first. If I run both batches with partition 1,
    # I'm expecting partition 1 will be marked as deleted
    run_own_connection(get_conn_callback, f"INSERT INTO {database}.{schema}.tmp_in (t, x, y) VALUES ('20210609T050000', 1, 1), ('20210614T050000', 2, 2);")
    snapshot(
            get_conn_callback,
            f'{database}.{schema}',
            'tmp_in',
            f'{database}.{schema}.tmp_in',
            f'{database}.{schema}.tmp_scd',
            f'{database}.{schema}.tmp_batch_metadata',
            'txy',
            'xy',
            't',
            '20210609T050000',
            partitions=({'x': 1},)
    )
    # a second round to stage the second batch 
    snapshot(
            get_conn_callback,
            f'{database}.{schema}',
            'tmp_in',
            f'{database}.{schema}.tmp_in',
            f'{database}.{schema}.tmp_scd',
            f'{database}.{schema}.tmp_batch_metadata',
            'txy',
            'xy',
            't',
            '20210614T050000',
            partitions=({'x': 1},)
    )
    rows = run_and_fetchall_own_connection(get_conn_callback, f'SELECT * FROM {database}.{schema}.tmp_scd')
    drop_tables(get_conn_callback, database, schema)
    print(rows)
    is_one_row = (len(rows) == 1)
    is_most_recent_always_true = all([row['_scd_is_most_recent'] for row in rows])
    is_deleted_always_true = all([row['_scd_deleted_timestamp'] is not None for row in rows])
    return _is_okay_invariants_scd_table(rows) and is_one_row and is_most_recent_always_true and is_deleted_always_true


def is_scenario_44_success(database, schema, get_conn_callback) -> bool:
    create_tables(get_conn_callback, database, schema, 't', {'x': 'INT', 'y': 'INT'})
    run_own_connection(get_conn_callback, f"INSERT INTO {database}.{schema}.tmp_in (t, x, y) VALUES ('20210609T050000', 1, 1), ('20210614T050000', 1, 2);")
    # Test additional join columns for the first time
    snapshot(
            get_conn_callback,
            f'{database}.{schema}',
            'tmp_in',
            f'{database}.{schema}.tmp_in',
            f'{database}.{schema}.tmp_scd',
            f'{database}.{schema}.tmp_batch_metadata',
            'txy',
            'xy',
            't',
            '20210609T050000',
            additional_join_columns=('x',)
    )
    # a second round to stage the second batch 
    snapshot(
            get_conn_callback,
            f'{database}.{schema}',
            'tmp_in',
            f'{database}.{schema}.tmp_in',
            f'{database}.{schema}.tmp_scd',
            f'{database}.{schema}.tmp_batch_metadata',
            'txy',
            'xy',
            't',
            '20210614T050000',
            additional_join_columns=('x',)
    )
    rows = run_and_fetchall_own_connection(get_conn_callback, f'SELECT * FROM {database}.{schema}.tmp_scd')
    drop_tables(get_conn_callback, database, schema)
    print(rows)
    is_two_rows = (len(rows) == 2)
    is_most_recent_always_true = all([row['_scd_is_most_recent'] for row in rows])
    return _is_okay_invariants_scd_table(rows) and is_two_rows and is_most_recent_always_true 


def is_scenario_45_success(database, schema, get_conn_callback) -> bool:
    """
    For the first time, test running the various steps ourselves
    """
    create_tables(get_conn_callback, database, schema, 't', {'x': 'INT', 'y': 'INT'})
    run_own_connection(get_conn_callback, f"INSERT INTO {database}.{schema}.tmp_in (t, x, y) VALUES ('20210609T050000', 1, 1), ('20210609T050000', 1, 2);")

    working_schema = f'{database}.{schema}'
    table_name = 'tmp_in'
    source_table = f'{database}.{schema}.tmp_in'
    scd_table = f'{database}.{schema}.tmp_scd'
    batch_metadata_table = f'{database}.{schema}.tmp_batch_metadata'
    columns = 'txy'
    unique_compound_key = 'xy'
    ts_nodash_col = 't'
    
    prep_batch(get_conn_callback, working_schema, table_name, scd_table, batch_metadata_table, ts_nodash_col, ts_nodash='20210609T050000', source_table=source_table)
    snapshot_partition(get_conn_callback, working_schema, table_name, scd_table, columns, unique_compound_key, ts_nodash_col)
    deploy_batch(get_conn_callback, working_schema, table_name, scd_table, batch_metadata_table)
    clean_up_batch(get_conn_callback, working_schema, table_name)

    rows = run_and_fetchall_own_connection(get_conn_callback, f'SELECT * FROM {database}.{schema}.tmp_scd')
    print(rows)
    is_two_rows = (len(rows) == 2)
    is_most_recent_always_true = all([row['_scd_is_most_recent'] for row in rows])

    # Now do a second run of the whole process, but, using ts_nodash=None
    # This one is supposed to run to completion without changing the output table at all
    prep_batch(get_conn_callback, working_schema, table_name, scd_table, batch_metadata_table, ts_nodash_col)
    snapshot_partition(get_conn_callback, working_schema, table_name, scd_table, columns, unique_compound_key, ts_nodash_col)
    deploy_batch(get_conn_callback, working_schema, table_name, scd_table, batch_metadata_table)
    clean_up_batch(get_conn_callback, working_schema, table_name)
    rows_after_second_idle_run = run_and_fetchall_own_connection(get_conn_callback, f'SELECT * FROM {database}.{schema}.tmp_scd')

    drop_tables(get_conn_callback, database, schema)
    return _is_okay_invariants_scd_table(rows) and is_two_rows and is_most_recent_always_true and rows == rows_after_second_idle_run




def is_scenario_46_success(database, schema, get_conn_callback) -> bool:
    """
    For the first time, test running concurrent partitions
    """
    import concurrent.futures
    create_tables(get_conn_callback, database, schema, 't', {'x': 'INT', 'y': 'INT'}, partition_columns=('x',))
    run_own_connection(get_conn_callback, f"""
        INSERT INTO {database}.{schema}.tmp_in
        (t, x, y)
        VALUES
        ('20210609T050000', 1, 1),
        ('20210609T050000', 2, 1),
        ('20210609T050000', 3, 1),
        ('20210609T050000', 4, 1),
        ('20210609T050000', 5, 1),
        ('20210609T050000', 6, 1),
        ('20210609T050000', 7, 1),
        ('20210609T050000', 8, 1),
        ('20210609T050000', 9, 1),
        ('20210609T050000', 10, 1),
        ('20210609T050000', 11, 1),
        ('20210609T050000', 12, 1),
        ('20210609T050000', 13, 1),
        ('20210609T050000', 14, 1),
        ('20210609T050000', 15, 1),
        ('20210609T050000', 16, 1),
        ('20210609T050000', 17, 1),
        ('20210609T050000', 18, 1),
        ('20210609T050000', 19, 1),
        ('20210609T050000', 20, 1)
        ;
        """)
    working_schema = f'{database}.{schema}'
    table_name = 'tmp_in'
    source_table = f'{database}.{schema}.tmp_in'
    scd_table = f'{database}.{schema}.tmp_scd'
    batch_metadata_table = f'{database}.{schema}.tmp_batch_metadata'
    columns = 'txy'
    unique_compound_key = 'xy'
    ts_nodash_col = 't'
    
    prep_batch(get_conn_callback, working_schema, table_name, scd_table, batch_metadata_table, ts_nodash_col, ts_nodash='20210609T050000',
        partitions=[{'x': x} for x in range(1, 21)], source_table=source_table)

    def _do_partition(partition):
        print(f'snapshotting partition {partition}')
        snapshot_partition(get_conn_callback, working_schema, table_name, scd_table, columns, unique_compound_key, ts_nodash_col, partition={'x': partition})

    with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
        executor.map(_do_partition, range(1, 21))

    deploy_batch(get_conn_callback, working_schema, table_name, scd_table, batch_metadata_table)
    clean_up_batch(get_conn_callback, working_schema, table_name)

    rows = run_and_fetchall_own_connection(get_conn_callback, f'SELECT * FROM {database}.{schema}.tmp_scd')
    print(rows)
    is_twenty_rows = (len(rows) == 20)
    is_most_recent_always_true = all([row['_scd_is_most_recent'] for row in rows])


    drop_tables(get_conn_callback, database, schema)
    return _is_okay_invariants_scd_table(rows) and is_twenty_rows and is_most_recent_always_true