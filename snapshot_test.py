from snowflake.connector import DictCursor, ProgrammingError
import logging


from util import run_and_fetchall, run
from test_unique_compound_key import test_unique_compound_key
from snapshot import snapshot


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


def create_tables(conn, database, schema, ts_nodash_col, non_ts_nodash_columns, partition_columns=None, how_to_create='OR REPLACE'):
    columns_str = ', '.join([f'{col} {type_}' for col, type_ in non_ts_nodash_columns.items()])
    partition_columns_str = '' if not partition_columns else ', ' + ', '.join([f'{col} {non_ts_nodash_columns[col]} NOT NULL' for col in partition_columns])
    run(conn, 'USE DATABASE IDENTIFIER(%(database)s);', params={'database': database})
    run(conn, 'USE SCHEMA IDENTIFIER(%(schema)s);', params={'schema': schema})
    run(conn, f'CREATE {how_to_create} TABLE tmp_in ({ts_nodash_col} VARCHAR, {columns_str});')
    run(conn, f'CREATE {how_to_create} TABLE tmp_stage '
              f'({ts_nodash_col} VARCHAR, {columns_str}'
              ', _scd_created_timestamp TIMESTAMP_NTZ NOT NULL DEFAULT SYSDATE()'
              ', _scd_normalised_key VARCHAR NOT NULL, _scd_created_by VARCHAR);')
    run(conn, f'CREATE {how_to_create} TABLE tmp_changes '
              f'({ts_nodash_col} VARCHAR, {columns_str}, _scd_normalised_key VARCHAR NOT NULL'
              ', _scd_id VARCHAR NOT NULL, _scd_valid_from_timestamp TIMESTAMP_NTZ NOT NULL, _scd_valid_to_timestamp TIMESTAMP_NTZ'
              ', _scd_deleted_timestamp TIMESTAMP_NTZ, _scd_is_most_recent BOOLEAN NOT NULL'
              ', _scd_last_dml_action VARCHAR, _scd_priority INT'
              ', _scd_created_timestamp TIMESTAMP_NTZ NOT NULL'
              ', _scd_last_modified_timestamp TIMESTAMP_NTZ NOT NULL'
              ', _scd_created_by VARCHAR, _scd_last_modified_by VARCHAR);')
    run(conn, f'CREATE {how_to_create} TABLE tmp_scd '
              f'({ts_nodash_col} VARCHAR, {columns_str}, _scd_normalised_key VARCHAR NOT NULL'
              ', _scd_id VARCHAR NOT NULL, _scd_valid_from_timestamp TIMESTAMP_NTZ NOT NULL, _scd_valid_to_timestamp TIMESTAMP_NTZ'
              ', _scd_deleted_timestamp TIMESTAMP_NTZ, _scd_is_most_recent BOOLEAN NOT NULL'
              ', _scd_last_dml_action VARCHAR'
              ', _scd_created_timestamp TIMESTAMP_NTZ NOT NULL'
              ', _scd_last_modified_timestamp TIMESTAMP_NTZ NOT NULL'
              ', _scd_created_by VARCHAR, _scd_last_modified_by VARCHAR);')
    run(conn, f'CREATE {how_to_create} TABLE tmp_batch_metadata ({ts_nodash_col} VARCHAR NOT NULL{partition_columns_str}'
              ', _scd_created_timestamp TIMESTAMP_NTZ NOT NULL DEFAULT SYSDATE(), _scd_created_by VARCHAR'
              ', _scd_batch_metadata VARIANT);')


 
def is_scenario_1_success(database, schema, get_conn_callback) -> bool:
    conn = get_conn_callback()
    create_tables(conn, database, schema, 't', {'x': 'INT', 'y': 'INT'})
    run(conn, "INSERT INTO tmp_in (t, x, y) VALUES ('20210609T050000', 1, 2), ('20210609T050000', 2, 1);")
    snapshot(conn, f'{database}.{schema}.tmp_batch_metadata', f'{database}.{schema}.tmp_in', f'{database}.{schema}.tmp_stage', f'{database}.{schema}.tmp_changes',
             f'{database}.{schema}.tmp_scd', 'txy', 'xy', 't', '20210609T050000')
    rows = run_and_fetchall(conn, 'SELECT * FROM tmp_scd')
    print(rows)
    is_two_rows = (len(rows) == 2)
    is_most_recent_always_true = all([row['_scd_is_most_recent'] for row in rows])
    conn.close()
    return _is_okay_invariants_scd_table(rows) and is_two_rows and is_most_recent_always_true 


def is_scenario_2_success(database, schema, get_conn_callback) -> bool:
    conn = get_conn_callback()
    create_tables(conn, database, schema, 't', {'x': 'INT', 'y': 'INT'})
    # introduce a duplicate row in the source table; only during the second step, where the changes are staged, will it be deduplicated.
    run(conn, "INSERT INTO tmp_in (t, x, y) VALUES ('20210609T050000', 1, 2), ('20210609T050000', 2, 1), ('20210609T050000', 2, 1);")
    snapshot(conn, f'{database}.{schema}.tmp_batch_metadata', f'{database}.{schema}.tmp_in', f'{database}.{schema}.tmp_stage', f'{database}.{schema}.tmp_changes',
             f'{database}.{schema}.tmp_scd', 'txy', 'xy', 't', '20210609T050000', fail_on_duplicates=False)
    rows = run_and_fetchall(conn, 'SELECT * FROM tmp_scd')
    print(rows)
    is_two_rows = (len(rows) == 2)
    is_most_recent_always_true = all([row['_scd_is_most_recent'] for row in rows])
    conn.close()
    return _is_okay_invariants_scd_table(rows) and is_two_rows and is_most_recent_always_true


def is_scenario_3_success(database, schema, get_conn_callback) -> bool:
    conn = get_conn_callback()
    create_tables(conn, database, schema, 't', {'x': 'INT', 'y': 'INT'}, partition_columns=('x',))
    run(conn, "INSERT INTO tmp_in (t, x, y) VALUES ('20210609T050000', 1, 1), ('20210609T050000', 1, 2), ('20210609T050000', 2, 1);")
    # use partitions
    snapshot(conn, f'{database}.{schema}.tmp_batch_metadata', f'{database}.{schema}.tmp_in', f'{database}.{schema}.tmp_stage', f'{database}.{schema}.tmp_changes',
             f'{database}.{schema}.tmp_scd', 'txy', 'xy', 't', '20210609T050000', partitions=({'x': 1},))
    rows = run_and_fetchall(conn, 'SELECT * FROM tmp_scd')
    print(rows)
    is_two_rows = (len(rows) == 2)
    is_most_recent_always_true = all([row['_scd_is_most_recent'] for row in rows])
    conn.close()
    return _is_okay_invariants_scd_table(rows) and is_two_rows and is_most_recent_always_true



def is_scenario_4_success(database, schema, get_conn_callback) -> bool:
    conn = get_conn_callback()
    create_tables(conn, database, schema, 't', {'x': 'INT', 'y': 'INT'})
    run(conn, "INSERT INTO tmp_in (t, x, y) VALUES ('20210609T050000', 1, 2), ('20210609T050000', 2, 1);")
    # use a column with values (y) that is not part of a unique key
    snapshot(conn, f'{database}.{schema}.tmp_batch_metadata', f'{database}.{schema}.tmp_in', f'{database}.{schema}.tmp_stage', f'{database}.{schema}.tmp_changes',
             f'{database}.{schema}.tmp_scd', 'txy', 'x', 't', '20210609T050000')
    rows = run_and_fetchall(conn, 'SELECT * FROM tmp_scd')
    print(rows)
    is_two_rows = (len(rows) == 2)
    is_most_recent_always_true = all([row['_scd_is_most_recent'] for row in rows])
    conn.close()
    return _is_okay_invariants_scd_table(rows) and is_two_rows and is_most_recent_always_true


def is_scenario_5_success(database, schema, get_conn_callback) -> bool:
    conn = get_conn_callback()
    create_tables(conn, database, schema, 't', {'x': 'INT', 'y': 'INT'})
    # now in this table there are two batches worth of data
    run(conn, "INSERT INTO tmp_in (t, x, y) VALUES ('20210609T050000', 1, 2), ('20210609T050000', 2, 1), "
              "('20210614T050000', 1, 1), ('20210614T050000', 2, 1);")
    snapshot(conn, f'{database}.{schema}.tmp_batch_metadata', f'{database}.{schema}.tmp_in', f'{database}.{schema}.tmp_stage', f'{database}.{schema}.tmp_changes',
             f'{database}.{schema}.tmp_scd', 'txy', 'x', 't', '20210609T050000')
    # a second round to stage the second batch 
    snapshot(conn, f'{database}.{schema}.tmp_batch_metadata', f'{database}.{schema}.tmp_in', f'{database}.{schema}.tmp_stage', f'{database}.{schema}.tmp_changes',
             f'{database}.{schema}.tmp_scd', 'txy', 'x', 't', '20210614T050000')
    rows = run_and_fetchall(conn, 'SELECT * FROM tmp_scd')
    print(rows)
    is_three_rows = (len(rows) == 3)
    is_most_recent_not_always_true = not all([row['_scd_is_most_recent'] for row in rows])
    conn.close()
    return _is_okay_invariants_scd_table(rows) and is_three_rows and is_most_recent_not_always_true


def is_scenario_6_success(database, schema, get_conn_callback) -> bool:
    conn = get_conn_callback()
    create_tables(conn, database, schema, 't', {'x': 'INT', 'y': 'INT'})
    # introduce a deletion in the second batch (remember, each batch is considered to be a full load)
    run(conn, "INSERT INTO tmp_in (t, x, y) VALUES ('20210609T050000', 1, 2), ('20210609T050000', 2, 1), "
              "('20210614T050000', 1, 1);")
    snapshot(conn, f'{database}.{schema}.tmp_batch_metadata', f'{database}.{schema}.tmp_in', f'{database}.{schema}.tmp_stage', f'{database}.{schema}.tmp_changes',
             f'{database}.{schema}.tmp_scd', 'txy', 'x', 't', '20210609T050000')
    # a second round to stage the second batch 
    snapshot(conn, f'{database}.{schema}.tmp_batch_metadata', f'{database}.{schema}.tmp_in', f'{database}.{schema}.tmp_stage', f'{database}.{schema}.tmp_changes',
             f'{database}.{schema}.tmp_scd', 'txy', 'x', 't', '20210614T050000')
    rows = run_and_fetchall(conn, 'SELECT * FROM tmp_scd')
    print(rows)
    is_three_rows = (len(rows) == 3)
    is_most_recent_not_always_true = not all([row['_scd_is_most_recent'] for row in rows])
    is_a_deleted_row_present = any([row['_scd_deleted_timestamp'] for row in rows])
    conn.close()
    return _is_okay_invariants_scd_table(rows) and is_three_rows and is_most_recent_not_always_true and is_a_deleted_row_present


def is_scenario_7_success(database, schema, get_conn_callback) -> bool:
    conn = get_conn_callback()
    create_tables(conn, database, schema, 't', {'x': 'INT', 'y': 'INT'})
    # introduce a deletion in the second batch (remember, each batch is considered to be a full load)
    # then, in the third batch, re-introduce a previously deleted row, without altering any of the values
    run(conn, "INSERT INTO tmp_in (t, x, y) VALUES ('20210609T050000', 1, 2), ('20210609T050000', 2, 1), "
              "('20210614T050000', 1, 1), "
              "('20210619T050000', 1, 1), ('20210619T050000', 2, 1);")
    snapshot(conn, f'{database}.{schema}.tmp_batch_metadata', f'{database}.{schema}.tmp_in', f'{database}.{schema}.tmp_stage', f'{database}.{schema}.tmp_changes',
             f'{database}.{schema}.tmp_scd', 'txy', 'x', 't', '20210609T050000')
    # a second round to stage the second batch 
    snapshot(conn, f'{database}.{schema}.tmp_batch_metadata', f'{database}.{schema}.tmp_in', f'{database}.{schema}.tmp_stage', f'{database}.{schema}.tmp_changes',
             f'{database}.{schema}.tmp_scd', 'txy', 'x', 't', '20210614T050000')
    # a third round to stage the third batch
    snapshot(conn, f'{database}.{schema}.tmp_batch_metadata', f'{database}.{schema}.tmp_in', f'{database}.{schema}.tmp_stage', f'{database}.{schema}.tmp_changes',
             f'{database}.{schema}.tmp_scd', 'txy', 'x', 't', '20210619T050000')
    rows = run_and_fetchall(conn, 'SELECT * FROM tmp_scd')
    print(rows)
    is_four_rows = (len(rows) == 4)
    is_most_recent_not_always_true = not all([row['_scd_is_most_recent'] for row in rows])
    is_a_deleted_row_present = any([row['_scd_deleted_timestamp'] for row in rows])
    is_a_deleted_invalidated_row_present = any([row['_scd_deleted_timestamp'] and not row['_scd_is_most_recent'] for row in rows])
    conn.close()
    return _is_okay_invariants_scd_table(rows) and is_four_rows and is_most_recent_not_always_true and is_a_deleted_row_present and is_a_deleted_invalidated_row_present



def is_scenario_8_success(database, schema, get_conn_callback) -> bool:
    conn = get_conn_callback()
    create_tables(conn, database, schema, 't', {'x': 'INT', 'y': 'INT'})
    # introduce NULL values in the values (y) and make sure it is not seen as an update (NULL = NULL evaluates to NULL in SQL)
    run(conn, "INSERT INTO tmp_in (t, x, y) VALUES ('20210609T050000', 1, NULL), ('20210614T050000', 1, NULL);")
    snapshot(conn, f'{database}.{schema}.tmp_batch_metadata', f'{database}.{schema}.tmp_in', f'{database}.{schema}.tmp_stage', f'{database}.{schema}.tmp_changes',
             f'{database}.{schema}.tmp_scd', 'txy', 'x', 't', '20210609T050000')
    # a second round to stage the second batch 
    snapshot(conn, f'{database}.{schema}.tmp_batch_metadata', f'{database}.{schema}.tmp_in', f'{database}.{schema}.tmp_stage', f'{database}.{schema}.tmp_changes',
             f'{database}.{schema}.tmp_scd', 'txy', 'x', 't', '20210614T050000')
    rows = run_and_fetchall(conn, 'SELECT * FROM tmp_scd')
    print(rows)
    is_one_row = (len(rows) == 1)
    is_most_recent_always_true = all([row['_scd_is_most_recent'] for row in rows])
    conn.close()
    return _is_okay_invariants_scd_table(rows) and is_one_row and is_most_recent_always_true


def is_scenario_9_success(database, schema, get_conn_callback) -> bool:
    conn = get_conn_callback()
    create_tables(conn, database, schema, 't', {'x': 'INT', 'y': 'INT'})
    # introduce NULL values in the unique compound key (x) and make sure it is treated as an actual value
    # (Sadly, in real life tables sometimes have no unique compound key, and we have to include columns that can be null in a "guessed" unique compound key in order to make 
    # rows unique in data observed so far.)
    run(conn, "INSERT INTO tmp_in (t, x, y) VALUES ('20210609T050000', NULL, 1);")
    snapshot(conn, f'{database}.{schema}.tmp_batch_metadata', f'{database}.{schema}.tmp_in', f'{database}.{schema}.tmp_stage', f'{database}.{schema}.tmp_changes',
             f'{database}.{schema}.tmp_scd', 'txy', 'x', 't', '20210609T050000')
    rows = run_and_fetchall(conn, 'SELECT * FROM tmp_scd')
    print(rows)
    is_one_row = (len(rows) == 1)
    is_most_recent_always_true = all([row['_scd_is_most_recent'] for row in rows])
    conn.close()
    return _is_okay_invariants_scd_table(rows) and is_one_row and is_most_recent_always_true


def is_scenario_10_success(database, schema, get_conn_callback) -> bool:
    conn = get_conn_callback()
    create_tables(conn, database, schema, 't', {'x': 'INT', 'y': 'INT'})
    # introduce NULL values in the unique compound key (x) and make sure it is treated as an actual value
    # (Sadly, in real life tables sometimes have no unique compound key, and we have to include columns that can be null in a "guessed" unique compound key in order to make 
    # rows unique in data observed so far.)
    # Also, add multiple rows with the same NULL compound key, and check that they are deduplicated as a single _scd_normalised_key (with an arbitrary row selected)
    run(conn, "INSERT INTO tmp_in (t, x, y) VALUES ('20210609T050000', NULL, 1), ('20210609T050000', NULL, 2);")
    snapshot(conn, f'{database}.{schema}.tmp_batch_metadata', f'{database}.{schema}.tmp_in', f'{database}.{schema}.tmp_stage', f'{database}.{schema}.tmp_changes',
             f'{database}.{schema}.tmp_scd', 'txy', 'x', 't', '20210609T050000', fail_on_duplicates=False)
    run(conn, 'COMMIT;')
    rows = run_and_fetchall(conn, 'SELECT * FROM tmp_scd')
    print(rows)
    is_one_row = (len(rows) == 1)
    is_most_recent_always_true = all([row['_scd_is_most_recent'] for row in rows])
    conn.close()
    return _is_okay_invariants_scd_table(rows) and is_one_row and is_most_recent_always_true
    

def is_scenario_11_success(database, schema, get_conn_callback) -> bool:
    conn = get_conn_callback()
    create_tables(conn, database, schema, 't', {'x': 'INT', 'y': 'INT'})
    # introduce NULL values in the unique compound key (x) and make sure it is treated as an actual value
    # (Sadly, in real life tables sometimes have no unique compound key, and we have to include columns that can be null in a "guessed" unique compound key in order to make 
    # rows unique in data observed so far.)
    # Also, check if we can update rows with a NULL value in a column in the compound key
    run(conn, "INSERT INTO tmp_in (t, x, y) VALUES ('20210609T050000', NULL, 1), ('20210614T050000', NULL, 2);")
    snapshot(conn, f'{database}.{schema}.tmp_batch_metadata', f'{database}.{schema}.tmp_in', f'{database}.{schema}.tmp_stage', f'{database}.{schema}.tmp_changes',
             f'{database}.{schema}.tmp_scd', 'txy', 'x', 't', '20210609T050000')
    # a second round to stage the second batch 
    snapshot(conn, f'{database}.{schema}.tmp_batch_metadata', f'{database}.{schema}.tmp_in', f'{database}.{schema}.tmp_stage', f'{database}.{schema}.tmp_changes',
             f'{database}.{schema}.tmp_scd', 'txy', 'x', 't', '20210614T050000')
    rows = run_and_fetchall(conn, 'SELECT * FROM tmp_scd')
    print(rows)
    is_two_rows = (len(rows) == 2)
    is_most_recent_not_always_true = not all([row['_scd_is_most_recent'] for row in rows])
    conn.close()
    return _is_okay_invariants_scd_table(rows) and is_two_rows and is_most_recent_not_always_true


def is_scenario_12_success(database, schema, get_conn_callback) -> bool:
    conn = get_conn_callback()
    create_tables(conn, database, schema, 't', {'x': 'INT', 'y': 'INT'}, partition_columns=('x',))
    # Check that partitions are handled okay, and that, if they are used, only rows in scd_table from those partitions will ever be marked for deletion
    run(conn, "INSERT INTO tmp_in (t, x, y) VALUES ('20210609T050000', 1, 1), ('20210614T050000', 2, 2);")
    snapshot(conn, f'{database}.{schema}.tmp_batch_metadata', f'{database}.{schema}.tmp_in', f'{database}.{schema}.tmp_stage', f'{database}.{schema}.tmp_changes',
             f'{database}.{schema}.tmp_scd', 'txy', 'xy', 't', '20210609T050000', partitions=({'x': 1},))
    # a second round to stage the second batch 
    snapshot(conn, f'{database}.{schema}.tmp_batch_metadata', f'{database}.{schema}.tmp_in', f'{database}.{schema}.tmp_stage', f'{database}.{schema}.tmp_changes',
             f'{database}.{schema}.tmp_scd', 'txy', 'xy', 't', '20210614T050000', partitions=({'x': 2},))
    rows = run_and_fetchall(conn, 'SELECT * FROM tmp_scd')
    print(rows)
    is_two_rows = (len(rows) == 2)
    is_most_recent_always_true = all([row['_scd_is_most_recent'] for row in rows])
    is_deleted_always_none = all([row['_scd_deleted_timestamp'] is None for row in rows])
    conn.close()
    return _is_okay_invariants_scd_table(rows) and is_two_rows and is_most_recent_always_true and is_deleted_always_none


def is_scenario_13_success(database, schema, get_conn_callback) -> bool:
    conn = get_conn_callback()
    create_tables(conn, database, schema, 't', {'x': 'INT', 'y': 'INT'}, partition_columns=('x', 'y'))
    run(conn, "INSERT INTO tmp_in (t, x, y) VALUES ('20210609T050000', 1, 2), ('20210609T050000', 3, 4), ('20210614T050000', 1, 2);")
    run(conn, 'BEGIN TRANSACTION;')
    snapshot(conn, f'{database}.{schema}.tmp_batch_metadata', f'{database}.{schema}.tmp_in', f'{database}.{schema}.tmp_stage', f'{database}.{schema}.tmp_changes',
             f'{database}.{schema}.tmp_scd', 'txy', 'xy', 't', '20210614T050000', partitions=({'x': 1, 'y': 2},))
    run(conn, 'COMMIT;')
    # a second round to stage the second set of partitions for a batch dated Jun 9; however, for one of the partitions in it a newer batch exists:
    is_protection_okay = False
    run(conn, 'BEGIN TRANSACTION;')
    try:  
        snapshot(conn, f'{database}.{schema}.tmp_batch_metadata', f'{database}.{schema}.tmp_in', f'{database}.{schema}.tmp_stage', f'{database}.{schema}.tmp_changes',
                 f'{database}.{schema}.tmp_scd', 'txy', 'xy', 't', '20210609T050000', partitions=({'x': 1, 'y': 2}, {'x': 3, 'y': 4}))
    except ValueError as e:
        print('--------------')
        print(e)
        print('--------------')
        if 'no earlier' in str(e):
            is_protection_okay = True
            run(conn, 'ROLLBACK;')
        else: 
            raise
    else:
        run(conn, 'COMMIT;')
    rows = run_and_fetchall(conn, 'SELECT * FROM tmp_scd')
    print(rows)
    is_one_row = (len(rows) == 1)
    is_most_recent_always_true = all([row['_scd_is_most_recent'] for row in rows])
    is_deleted_always_none = all([row['_scd_deleted_timestamp'] is None for row in rows])
    conn.close()
    return _is_okay_invariants_scd_table(rows) and is_one_row and is_most_recent_always_true and is_deleted_always_none and is_protection_okay


def is_scenario_14_success(database, schema, get_conn_callback) -> bool:
    conn = get_conn_callback()
    create_tables(conn, database, schema, 't', {'x': 'INT', 'y': 'INT'})
    run(conn, "INSERT INTO tmp_in (t, x, y) VALUES ('20210609T050000', 1, 2), ('20210609T050000', 2, 1);")
    snapshot(conn, f'{database}.{schema}.tmp_batch_metadata', f'{database}.{schema}.tmp_in', f'{database}.{schema}.tmp_stage', f'{database}.{schema}.tmp_changes',
             f'{database}.{schema}.tmp_scd', 'txy', 'xy', 't', '20210609T050000', batch_metadata={'source': 'ONE_TIME_DUMP'})
    rows = run_and_fetchall(conn, 'SELECT * FROM tmp_scd')
    print(rows)
    is_two_rows = (len(rows) == 2)
    is_most_recent_always_true = all([row['_scd_is_most_recent'] for row in rows])
    conn.close()
    return _is_okay_invariants_scd_table(rows) and is_two_rows and is_most_recent_always_true 


def is_scenario_15_success(database, schema, get_conn_callback) -> bool:
    conn = get_conn_callback()
    create_tables(conn, database, schema, 't', {'x': 'INT', 'y': 'INT'}, partition_columns=('x',))
    run(conn, "INSERT INTO tmp_in (t, x, y) VALUES ('20210609T050000', 1, 1), ('20210609T050000', 1, 2), ('20210609T050000', 2, 1);")
    # use partitions with batch metadata, this time multiple partitions
    snapshot(conn, f'{database}.{schema}.tmp_batch_metadata', f'{database}.{schema}.tmp_in', f'{database}.{schema}.tmp_stage', f'{database}.{schema}.tmp_changes',
             f'{database}.{schema}.tmp_scd', 'txy', 'xy', 't', '20210609T050000', partitions=({'x': 1}, {'x': 2}), batch_metadata={'source': 'NEW_YORK_TIMES'},
             run_id='my_test_run')
    rows = run_and_fetchall(conn, 'SELECT * FROM tmp_scd')
    print(rows)
    is_three_rows = (len(rows) == 3)
    is_most_recent_always_true = all([row['_scd_is_most_recent'] for row in rows])
    conn.close()
    return _is_okay_invariants_scd_table(rows) and is_three_rows and is_most_recent_always_true


def is_scenario_16_success(database, schema, get_conn_callback) -> bool:
    conn = get_conn_callback()
    create_tables(conn, database, schema, 't', {'x': 'INT', 'y': 'INT'}, partition_columns=('x', 'y'))
    run(conn, "INSERT INTO tmp_in (t, x, y) VALUES ('20210609T050000', 1, 1), ('20210609T050000', 1, 2), ('20210609T050000', 2, 1);")
    # use partitions, two partition columns this time
    snapshot(conn, f'{database}.{schema}.tmp_batch_metadata', f'{database}.{schema}.tmp_in', f'{database}.{schema}.tmp_stage', f'{database}.{schema}.tmp_changes',
             f'{database}.{schema}.tmp_scd', 'txy', 'xy', 't', '20210609T050000', partitions=({'x': 1, 'y': 2},), batch_metadata={'source': 'WEEKLY'})
    rows = run_and_fetchall(conn, 'SELECT * FROM tmp_scd')
    print(rows)
    is_one_row = (len(rows) == 1)
    is_most_recent_always_true = all([row['_scd_is_most_recent'] for row in rows])
    conn.close()
    return _is_okay_invariants_scd_table(rows) and is_one_row and is_most_recent_always_true


def is_scenario_17_success(database, schema, get_conn_callback) -> bool:
    conn = get_conn_callback()
    create_tables(conn, database, schema, 't_', {'x': 'INT', 'y': 'INT'}, partition_columns=('x', 'y'))
    run(conn, "INSERT INTO tmp_in (t_, x, y) VALUES ('20210609T050000', 1, 1), ('20210609T050000', 1, 2), ('20210609T050000', 2, 1);")
    # Use lists or tuples (like we're supposed to) instead of strings that Python gladly iterates over (Python is a toy language)
    # Also use a multi letter ts_nodash_col to surface a silly bug that existed once in snapshot
    snapshot(conn, f'{database}.{schema}.tmp_batch_metadata', f'{database}.{schema}.tmp_in', f'{database}.{schema}.tmp_stage', f'{database}.{schema}.tmp_changes',
             f'{database}.{schema}.tmp_scd', ('t_', 'x', 'y'), ('x', 'y'), 't_', '20210609T050000', partitions=({'x': 1, 'y': 2},), batch_metadata={'source': 'WEEKLY'})
    rows = run_and_fetchall(conn, 'SELECT * FROM tmp_scd')
    print(rows)
    is_one_row = (len(rows) == 1)
    is_most_recent_always_true = all([row['_scd_is_most_recent'] for row in rows])
    conn.close()
    return _is_okay_invariants_scd_table(rows) and is_one_row and is_most_recent_always_true
    

def is_scenario_18_success(database, schema, get_conn_callback) -> bool:
    conn = get_conn_callback()
    # use a NOT NULL constraint in one of the columns to surface a bug we had in inserting the deletions in the changes table
    create_tables(conn, database, schema, 't', {'x': 'INT NOT NULL', 'y': 'INT'})
    # introduce a deletion in the second batch (remember, each batch is considered to be a full load)
    run(conn, "INSERT INTO tmp_in (t, x, y) VALUES ('20210609T050000', 1, 2), ('20210609T050000', 2, 1), "
              "('20210614T050000', 1, 1);")
    snapshot(conn, f'{database}.{schema}.tmp_batch_metadata', f'{database}.{schema}.tmp_in', f'{database}.{schema}.tmp_stage', f'{database}.{schema}.tmp_changes',
             f'{database}.{schema}.tmp_scd', 'txy', 'x', 't', '20210609T050000')
    # a second round to stage the second batch 
    snapshot(conn, f'{database}.{schema}.tmp_batch_metadata', f'{database}.{schema}.tmp_in', f'{database}.{schema}.tmp_stage', f'{database}.{schema}.tmp_changes',
             f'{database}.{schema}.tmp_scd', 'txy', 'x', 't', '20210614T050000')
    rows = run_and_fetchall(conn, 'SELECT * FROM tmp_scd')
    print(rows)
    is_three_rows = (len(rows) == 3)
    is_most_recent_not_always_true = not all([row['_scd_is_most_recent'] for row in rows])
    is_a_deleted_row_present = any([row['_scd_deleted_timestamp'] for row in rows])
    conn.close()
    return _is_okay_invariants_scd_table(rows) and is_three_rows and is_most_recent_not_always_true and is_a_deleted_row_present


def is_scenario_19_success(database, schema, get_conn_callback) -> bool:
    conn = get_conn_callback()
    create_tables(conn, database, schema, 't', {'x': 'INT', 'y': 'INT'}, partition_columns=('x',))
    # Check that a new row is only inserted for the partition it belongs to
    run(conn, "INSERT INTO tmp_in (t, x, y) VALUES ('20210609T050000', 1, 1), ('20210614T050000', 2, 2), ('20210629T050000', 2, 2), ('20210629T050000', 2, 3);")
    snapshot(conn, f'{database}.{schema}.tmp_batch_metadata', f'{database}.{schema}.tmp_in', f'{database}.{schema}.tmp_stage', f'{database}.{schema}.tmp_changes',
             f'{database}.{schema}.tmp_scd', 'txy', 'xy', 't', '20210609T050000', partitions=({'x': 1},))
    # a second round to stage the second batch 
    snapshot(conn, f'{database}.{schema}.tmp_batch_metadata', f'{database}.{schema}.tmp_in', f'{database}.{schema}.tmp_stage', f'{database}.{schema}.tmp_changes',
             f'{database}.{schema}.tmp_scd', 'txy', 'xy', 't', '20210614T050000', partitions=({'x': 2},))
    # a third round to stage the third batch 
    snapshot(conn, f'{database}.{schema}.tmp_batch_metadata', f'{database}.{schema}.tmp_in', f'{database}.{schema}.tmp_stage', f'{database}.{schema}.tmp_changes',
             f'{database}.{schema}.tmp_scd', 'txy', 'xy', 't', '20210629T050000', partitions=({'x': 2},))
    rows = run_and_fetchall(conn, 'SELECT * FROM tmp_scd')
    print(rows)
    is_three_rows = (len(rows) == 3)
    is_most_recent_always_true = all([row['_scd_is_most_recent'] for row in rows])
    is_deleted_always_none = all([row['_scd_deleted_timestamp'] is None for row in rows])
    conn.close()
    return _is_okay_invariants_scd_table(rows) and is_three_rows and is_most_recent_always_true and is_deleted_always_none


def is_scenario_20_success(database, schema, get_conn_callback) -> bool:
    conn = get_conn_callback()
    create_tables(conn, database, schema, 't', {'x': 'INT', 'y': 'INT'}, partition_columns=('x',))
    # Check that a new but unchanged row does not cause an insert of the new row + an invalidation of the old row
    run(conn, "INSERT INTO tmp_in (t, x, y) VALUES ('20210614T050000', 2, 2), ('20210629T050000', 2, 2);")
    snapshot(conn, f'{database}.{schema}.tmp_batch_metadata', f'{database}.{schema}.tmp_in', f'{database}.{schema}.tmp_stage', f'{database}.{schema}.tmp_changes',
             f'{database}.{schema}.tmp_scd', 'txy', 'xy', 't', '20210614T050000', partitions=({'x': 2},))
    # a second round to stage the second batch 
    snapshot(conn, f'{database}.{schema}.tmp_batch_metadata', f'{database}.{schema}.tmp_in', f'{database}.{schema}.tmp_stage', f'{database}.{schema}.tmp_changes',
             f'{database}.{schema}.tmp_scd', 'txy', 'xy', 't', '20210629T050000', partitions=({'x': 2},))
    rows = run_and_fetchall(conn, 'SELECT * FROM tmp_scd')
    print(rows)
    is_one_row = (len(rows) == 1)
    is_most_recent_always_true = all([row['_scd_is_most_recent'] for row in rows])
    is_deleted_always_none = all([row['_scd_deleted_timestamp'] is None for row in rows])
    conn.close()
    return _is_okay_invariants_scd_table(rows) and is_one_row and is_most_recent_always_true and is_deleted_always_none


# ========================================== #
# Run all scenario's also without zombie check
# ========================================== #
def is_scenario_21_success(database, schema, get_conn_callback) -> bool:
    conn = get_conn_callback()
    create_tables(conn, database, schema, 't', {'x': 'INT', 'y': 'INT'})
    run(conn, "INSERT INTO tmp_in (t, x, y) VALUES ('20210609T050000', 1, 2), ('20210609T050000', 2, 1);")
    snapshot(conn, f'{database}.{schema}.tmp_batch_metadata', f'{database}.{schema}.tmp_in', f'{database}.{schema}.tmp_stage', f'{database}.{schema}.tmp_changes',
             f'{database}.{schema}.tmp_scd', 'txy', 'xy', 't', '20210609T050000', do_zombie_check=False)
    rows = run_and_fetchall(conn, 'SELECT * FROM tmp_scd')
    print(rows)
    is_two_rows = (len(rows) == 2)
    is_most_recent_always_true = all([row['_scd_is_most_recent'] for row in rows])
    conn.close()
    return _is_okay_invariants_scd_table(rows, False) and is_two_rows and is_most_recent_always_true 


def is_scenario_22_success(database, schema, get_conn_callback) -> bool:
    conn = get_conn_callback()
    create_tables(conn, database, schema, 't', {'x': 'INT', 'y': 'INT'})
    # introduce a duplicate row in the source table; only during the second step, where the changes are staged, will it be deduplicated.
    run(conn, "INSERT INTO tmp_in (t, x, y) VALUES ('20210609T050000', 1, 2), ('20210609T050000', 2, 1), ('20210609T050000', 2, 1);")
    snapshot(conn, f'{database}.{schema}.tmp_batch_metadata', f'{database}.{schema}.tmp_in', f'{database}.{schema}.tmp_stage', f'{database}.{schema}.tmp_changes',
             f'{database}.{schema}.tmp_scd', 'txy', 'xy', 't', '20210609T050000', fail_on_duplicates=False, do_zombie_check=False)
    rows = run_and_fetchall(conn, 'SELECT * FROM tmp_scd')
    print(rows)
    is_two_rows = (len(rows) == 2)
    is_most_recent_always_true = all([row['_scd_is_most_recent'] for row in rows])
    conn.close()
    return _is_okay_invariants_scd_table(rows, False) and is_two_rows and is_most_recent_always_true


def is_scenario_23_success(database, schema, get_conn_callback) -> bool:
    conn = get_conn_callback()
    create_tables(conn, database, schema, 't', {'x': 'INT', 'y': 'INT'}, partition_columns=('x',))
    run(conn, "INSERT INTO tmp_in (t, x, y) VALUES ('20210609T050000', 1, 1), ('20210609T050000', 1, 2), ('20210609T050000', 2, 1);")
    # use partitions
    snapshot(conn, f'{database}.{schema}.tmp_batch_metadata', f'{database}.{schema}.tmp_in', f'{database}.{schema}.tmp_stage', f'{database}.{schema}.tmp_changes',
             f'{database}.{schema}.tmp_scd', 'txy', 'xy', 't', '20210609T050000', partitions=({'x': 1},), do_zombie_check=False)
    rows = run_and_fetchall(conn, 'SELECT * FROM tmp_scd')
    print(rows)
    is_two_rows = (len(rows) == 2)
    is_most_recent_always_true = all([row['_scd_is_most_recent'] for row in rows])
    conn.close()
    return _is_okay_invariants_scd_table(rows, False) and is_two_rows and is_most_recent_always_true



def is_scenario_24_success(database, schema, get_conn_callback) -> bool:
    conn = get_conn_callback()
    create_tables(conn, database, schema, 't', {'x': 'INT', 'y': 'INT'})
    run(conn, "INSERT INTO tmp_in (t, x, y) VALUES ('20210609T050000', 1, 2), ('20210609T050000', 2, 1);")
    # use a column with values (y) that is not part of a unique key
    snapshot(conn, f'{database}.{schema}.tmp_batch_metadata', f'{database}.{schema}.tmp_in', f'{database}.{schema}.tmp_stage', f'{database}.{schema}.tmp_changes',
             f'{database}.{schema}.tmp_scd', 'txy', 'x', 't', '20210609T050000', do_zombie_check=False)
    rows = run_and_fetchall(conn, 'SELECT * FROM tmp_scd')
    print(rows)
    is_two_rows = (len(rows) == 2)
    is_most_recent_always_true = all([row['_scd_is_most_recent'] for row in rows])
    conn.close()
    return _is_okay_invariants_scd_table(rows, False) and is_two_rows and is_most_recent_always_true


def is_scenario_25_success(database, schema, get_conn_callback) -> bool:
    conn = get_conn_callback()
    create_tables(conn, database, schema, 't', {'x': 'INT', 'y': 'INT'})
    # now in this table there are two batches worth of data
    run(conn, "INSERT INTO tmp_in (t, x, y) VALUES ('20210609T050000', 1, 2), ('20210609T050000', 2, 1), "
              "('20210614T050000', 1, 1), ('20210614T050000', 2, 1);")
    snapshot(conn, f'{database}.{schema}.tmp_batch_metadata', f'{database}.{schema}.tmp_in', f'{database}.{schema}.tmp_stage', f'{database}.{schema}.tmp_changes',
             f'{database}.{schema}.tmp_scd', 'txy', 'x', 't', '20210609T050000', do_zombie_check=False)
    # a second round to stage the second batch 
    snapshot(conn, f'{database}.{schema}.tmp_batch_metadata', f'{database}.{schema}.tmp_in', f'{database}.{schema}.tmp_stage', f'{database}.{schema}.tmp_changes',
             f'{database}.{schema}.tmp_scd', 'txy', 'x', 't', '20210614T050000', do_zombie_check=False)
    rows = run_and_fetchall(conn, 'SELECT * FROM tmp_scd')
    print(rows)
    is_three_rows = (len(rows) == 3)
    is_most_recent_not_always_true = not all([row['_scd_is_most_recent'] for row in rows])
    conn.close()
    return _is_okay_invariants_scd_table(rows, False) and is_three_rows and is_most_recent_not_always_true


def is_scenario_26_success(database, schema, get_conn_callback) -> bool:
    conn = get_conn_callback()
    create_tables(conn, database, schema, 't', {'x': 'INT', 'y': 'INT'})
    # introduce a deletion in the second batch (remember, each batch is considered to be a full load)
    run(conn, "INSERT INTO tmp_in (t, x, y) VALUES ('20210609T050000', 1, 2), ('20210609T050000', 2, 1), "
              "('20210614T050000', 1, 1);")
    snapshot(conn, f'{database}.{schema}.tmp_batch_metadata', f'{database}.{schema}.tmp_in', f'{database}.{schema}.tmp_stage', f'{database}.{schema}.tmp_changes',
             f'{database}.{schema}.tmp_scd', 'txy', 'x', 't', '20210609T050000', do_zombie_check=False)
    # a second round to stage the second batch 
    snapshot(conn, f'{database}.{schema}.tmp_batch_metadata', f'{database}.{schema}.tmp_in', f'{database}.{schema}.tmp_stage', f'{database}.{schema}.tmp_changes',
             f'{database}.{schema}.tmp_scd', 'txy', 'x', 't', '20210614T050000', do_zombie_check=False)
    rows = run_and_fetchall(conn, 'SELECT * FROM tmp_scd')
    print(rows)
    is_three_rows = (len(rows) == 3)
    is_most_recent_not_always_true = not all([row['_scd_is_most_recent'] for row in rows])
    is_a_deleted_row_present = any([row['_scd_deleted_timestamp'] for row in rows])
    conn.close()
    return _is_okay_invariants_scd_table(rows, False) and is_three_rows and is_most_recent_not_always_true and is_a_deleted_row_present


def is_scenario_27_success(database, schema, get_conn_callback) -> bool:
    conn = get_conn_callback()
    create_tables(conn, database, schema, 't', {'x': 'INT', 'y': 'INT'})
    # introduce a deletion in the second batch (remember, each batch is considered to be a full load)
    # then, in the third batch, re-introduce a previously deleted row, without altering any of the values
    # In other words, introduce a zombie, but without doing a zombie check. The result will be a duplicate valid row for an SCD
    # I wonder if have an invariant check already that returns False
    run(conn, "INSERT INTO tmp_in (t, x, y) VALUES ('20210609T050000', 1, 2), ('20210609T050000', 2, 1), "
              "('20210614T050000', 1, 1), "
              "('20210619T050000', 1, 1), ('20210619T050000', 2, 1);")
    snapshot(conn, f'{database}.{schema}.tmp_batch_metadata', f'{database}.{schema}.tmp_in', f'{database}.{schema}.tmp_stage', f'{database}.{schema}.tmp_changes',
             f'{database}.{schema}.tmp_scd', 'txy', 'x', 't', '20210609T050000', do_zombie_check=False)
    # a second round to stage the second batch 
    snapshot(conn, f'{database}.{schema}.tmp_batch_metadata', f'{database}.{schema}.tmp_in', f'{database}.{schema}.tmp_stage', f'{database}.{schema}.tmp_changes',
             f'{database}.{schema}.tmp_scd', 'txy', 'x', 't', '20210614T050000', do_zombie_check=False)
    # a third round to stage the third batch
    snapshot(conn, f'{database}.{schema}.tmp_batch_metadata', f'{database}.{schema}.tmp_in', f'{database}.{schema}.tmp_stage', f'{database}.{schema}.tmp_changes',
             f'{database}.{schema}.tmp_scd', 'txy', 'x', 't', '20210619T050000', do_zombie_check=False)
    rows = run_and_fetchall(conn, 'SELECT * FROM tmp_scd')
    print(rows)
    is_four_rows = (len(rows) == 4)
    is_most_recent_not_always_true = not all([row['_scd_is_most_recent'] for row in rows])
    is_a_deleted_row_present = any([row['_scd_deleted_timestamp'] for row in rows])
    conn.close()
    return _is_okay_invariants_scd_table(rows, False) and is_four_rows and is_most_recent_not_always_true and is_a_deleted_row_present



def is_scenario_28_success(database, schema, get_conn_callback) -> bool:
    conn = get_conn_callback()
    create_tables(conn, database, schema, 't', {'x': 'INT', 'y': 'INT'})
    # introduce NULL values in the values (y) and make sure it is not seen as an update (NULL = NULL evaluates to NULL in SQL)
    run(conn, "INSERT INTO tmp_in (t, x, y) VALUES ('20210609T050000', 1, NULL), ('20210614T050000', 1, NULL);")
    snapshot(conn, f'{database}.{schema}.tmp_batch_metadata', f'{database}.{schema}.tmp_in', f'{database}.{schema}.tmp_stage', f'{database}.{schema}.tmp_changes',
             f'{database}.{schema}.tmp_scd', 'txy', 'x', 't', '20210609T050000', do_zombie_check=False)
    # a second round to stage the second batch 
    snapshot(conn, f'{database}.{schema}.tmp_batch_metadata', f'{database}.{schema}.tmp_in', f'{database}.{schema}.tmp_stage', f'{database}.{schema}.tmp_changes',
             f'{database}.{schema}.tmp_scd', 'txy', 'x', 't', '20210614T050000', do_zombie_check=False)
    rows = run_and_fetchall(conn, 'SELECT * FROM tmp_scd')
    print(rows)
    is_one_row = (len(rows) == 1)
    is_most_recent_always_true = all([row['_scd_is_most_recent'] for row in rows])
    conn.close()
    return _is_okay_invariants_scd_table(rows, False) and is_one_row and is_most_recent_always_true


def is_scenario_29_success(database, schema, get_conn_callback) -> bool:
    conn = get_conn_callback()
    create_tables(conn, database, schema, 't', {'x': 'INT', 'y': 'INT'})
    # introduce NULL values in the unique compound key (x) and make sure it is treated as an actual value
    # (Sadly, in real life tables sometimes have no unique compound key, and we have to include columns that can be null in a "guessed" unique compound key in order to make 
    # rows unique in data observed so far.)
    run(conn, "INSERT INTO tmp_in (t, x, y) VALUES ('20210609T050000', NULL, 1);")
    snapshot(conn, f'{database}.{schema}.tmp_batch_metadata', f'{database}.{schema}.tmp_in', f'{database}.{schema}.tmp_stage', f'{database}.{schema}.tmp_changes',
             f'{database}.{schema}.tmp_scd', 'txy', 'x', 't', '20210609T050000', do_zombie_check=False)
    rows = run_and_fetchall(conn, 'SELECT * FROM tmp_scd')
    print(rows)
    is_one_row = (len(rows) == 1)
    is_most_recent_always_true = all([row['_scd_is_most_recent'] for row in rows])
    conn.close()
    return _is_okay_invariants_scd_table(rows, False) and is_one_row and is_most_recent_always_true


def is_scenario_30_success(database, schema, get_conn_callback) -> bool:
    conn = get_conn_callback()
    create_tables(conn, database, schema, 't', {'x': 'INT', 'y': 'INT'})
    # introduce NULL values in the unique compound key (x) and make sure it is treated as an actual value
    # (Sadly, in real life tables sometimes have no unique compound key, and we have to include columns that can be null in a "guessed" unique compound key in order to make 
    # rows unique in data observed so far.)
    # Also, add multiple rows with the same NULL compound key, and check that they are deduplicated as a single _scd_normalised_key (with an arbitrary row selected)
    run(conn, "INSERT INTO tmp_in (t, x, y) VALUES ('20210609T050000', NULL, 1), ('20210609T050000', NULL, 2);")
    snapshot(conn, f'{database}.{schema}.tmp_batch_metadata', f'{database}.{schema}.tmp_in', f'{database}.{schema}.tmp_stage', f'{database}.{schema}.tmp_changes',
             f'{database}.{schema}.tmp_scd', 'txy', 'x', 't', '20210609T050000', fail_on_duplicates=False, do_zombie_check=False)
    run(conn, 'COMMIT;')
    rows = run_and_fetchall(conn, 'SELECT * FROM tmp_scd')
    print(rows)
    is_one_row = (len(rows) == 1)
    is_most_recent_always_true = all([row['_scd_is_most_recent'] for row in rows])
    conn.close()
    return _is_okay_invariants_scd_table(rows, False) and is_one_row and is_most_recent_always_true
    

def is_scenario_31_success(database, schema, get_conn_callback) -> bool:
    conn = get_conn_callback()
    create_tables(conn, database, schema, 't', {'x': 'INT', 'y': 'INT'})
    # introduce NULL values in the unique compound key (x) and make sure it is treated as an actual value
    # (Sadly, in real life tables sometimes have no unique compound key, and we have to include columns that can be null in a "guessed" unique compound key in order to make 
    # rows unique in data observed so far.)
    # Also, check if we can update rows with a NULL value in a column in the compound key
    run(conn, "INSERT INTO tmp_in (t, x, y) VALUES ('20210609T050000', NULL, 1), ('20210614T050000', NULL, 2);")
    snapshot(conn, f'{database}.{schema}.tmp_batch_metadata', f'{database}.{schema}.tmp_in', f'{database}.{schema}.tmp_stage', f'{database}.{schema}.tmp_changes',
             f'{database}.{schema}.tmp_scd', 'txy', 'x', 't', '20210609T050000', do_zombie_check=False)
    # a second round to stage the second batch 
    snapshot(conn, f'{database}.{schema}.tmp_batch_metadata', f'{database}.{schema}.tmp_in', f'{database}.{schema}.tmp_stage', f'{database}.{schema}.tmp_changes',
             f'{database}.{schema}.tmp_scd', 'txy', 'x', 't', '20210614T050000', do_zombie_check=False)
    rows = run_and_fetchall(conn, 'SELECT * FROM tmp_scd')
    print(rows)
    is_two_rows = (len(rows) == 2)
    is_most_recent_not_always_true = not all([row['_scd_is_most_recent'] for row in rows])
    conn.close()
    return _is_okay_invariants_scd_table(rows, False) and is_two_rows and is_most_recent_not_always_true


def is_scenario_32_success(database, schema, get_conn_callback) -> bool:
    conn = get_conn_callback()
    create_tables(conn, database, schema, 't', {'x': 'INT', 'y': 'INT'}, partition_columns=('x',))
    # Check that partitions are handled okay, and that, if they are used, only rows in scd_table from those partitions will ever be marked for deletion
    run(conn, "INSERT INTO tmp_in (t, x, y) VALUES ('20210609T050000', 1, 1), ('20210614T050000', 2, 2);")
    snapshot(conn, f'{database}.{schema}.tmp_batch_metadata', f'{database}.{schema}.tmp_in', f'{database}.{schema}.tmp_stage', f'{database}.{schema}.tmp_changes',
             f'{database}.{schema}.tmp_scd', 'txy', 'xy', 't', '20210609T050000', partitions=({'x': 1},), do_zombie_check=False)
    # a second round to stage the second batch 
    snapshot(conn, f'{database}.{schema}.tmp_batch_metadata', f'{database}.{schema}.tmp_in', f'{database}.{schema}.tmp_stage', f'{database}.{schema}.tmp_changes',
             f'{database}.{schema}.tmp_scd', 'txy', 'xy', 't', '20210614T050000', partitions=({'x': 2},), do_zombie_check=False)
    rows = run_and_fetchall(conn, 'SELECT * FROM tmp_scd')
    print(rows)
    is_two_rows = (len(rows) == 2)
    is_most_recent_always_true = all([row['_scd_is_most_recent'] for row in rows])
    is_deleted_always_none = all([row['_scd_deleted_timestamp'] is None for row in rows])
    conn.close()
    return _is_okay_invariants_scd_table(rows, False) and is_two_rows and is_most_recent_always_true and is_deleted_always_none


def is_scenario_33_success(database, schema, get_conn_callback) -> bool:
    conn = get_conn_callback()
    create_tables(conn, database, schema, 't', {'x': 'INT', 'y': 'INT'}, partition_columns=('x', 'y'))
    run(conn, "INSERT INTO tmp_in (t, x, y) VALUES ('20210609T050000', 1, 2), ('20210609T050000', 3, 4), ('20210614T050000', 1, 2);")
    run(conn, 'BEGIN TRANSACTION;')
    snapshot(conn, f'{database}.{schema}.tmp_batch_metadata', f'{database}.{schema}.tmp_in', f'{database}.{schema}.tmp_stage', f'{database}.{schema}.tmp_changes',
             f'{database}.{schema}.tmp_scd', 'txy', 'xy', 't', '20210614T050000', partitions=({'x': 1, 'y': 2},), do_zombie_check=False)
    run(conn, 'COMMIT;')
    # a second round to stage the second set of partitions for a batch dated Jun 9; however, for one of the partitions in it a newer batch exists:
    is_protection_okay = False
    run(conn, 'BEGIN TRANSACTION;')
    try:  
        snapshot(conn, f'{database}.{schema}.tmp_batch_metadata', f'{database}.{schema}.tmp_in', f'{database}.{schema}.tmp_stage', f'{database}.{schema}.tmp_changes',
                 f'{database}.{schema}.tmp_scd', 'txy', 'xy', 't', '20210609T050000', partitions=({'x': 1, 'y': 2}, {'x': 3, 'y': 4}), do_zombie_check=False)
    except ValueError as e:
        print('--------------')
        print(e)
        print('--------------')
        if 'no earlier' in str(e):
            is_protection_okay = True
            run(conn, 'ROLLBACK;')
        else: 
            raise
    else:
        run(conn, 'COMMIT;')
    rows = run_and_fetchall(conn, 'SELECT * FROM tmp_scd')
    print(rows)
    is_one_row = (len(rows) == 1)
    is_most_recent_always_true = all([row['_scd_is_most_recent'] for row in rows])
    is_deleted_always_none = all([row['_scd_deleted_timestamp'] is None for row in rows])
    conn.close()
    return _is_okay_invariants_scd_table(rows, False) and is_one_row and is_most_recent_always_true and is_deleted_always_none and is_protection_okay


def is_scenario_34_success(database, schema, get_conn_callback) -> bool:
    conn = get_conn_callback()
    create_tables(conn, database, schema, 't', {'x': 'INT', 'y': 'INT'})
    run(conn, "INSERT INTO tmp_in (t, x, y) VALUES ('20210609T050000', 1, 2), ('20210609T050000', 2, 1);")
    snapshot(conn, f'{database}.{schema}.tmp_batch_metadata', f'{database}.{schema}.tmp_in', f'{database}.{schema}.tmp_stage', f'{database}.{schema}.tmp_changes',
             f'{database}.{schema}.tmp_scd', 'txy', 'xy', 't', '20210609T050000', batch_metadata={'source': 'ONE_TIME_DUMP'}, do_zombie_check=False)
    rows = run_and_fetchall(conn, 'SELECT * FROM tmp_scd')
    print(rows)
    is_two_rows = (len(rows) == 2)
    is_most_recent_always_true = all([row['_scd_is_most_recent'] for row in rows])
    conn.close()
    return _is_okay_invariants_scd_table(rows, False) and is_two_rows and is_most_recent_always_true 


def is_scenario_35_success(database, schema, get_conn_callback) -> bool:
    conn = get_conn_callback()
    create_tables(conn, database, schema, 't', {'x': 'INT', 'y': 'INT'}, partition_columns=('x',))
    run(conn, "INSERT INTO tmp_in (t, x, y) VALUES ('20210609T050000', 1, 1), ('20210609T050000', 1, 2), ('20210609T050000', 2, 1);")
    # use partitions with batch metadata, this time multiple partitions
    snapshot(conn, f'{database}.{schema}.tmp_batch_metadata', f'{database}.{schema}.tmp_in', f'{database}.{schema}.tmp_stage', f'{database}.{schema}.tmp_changes',
             f'{database}.{schema}.tmp_scd', 'txy', 'xy', 't', '20210609T050000', partitions=({'x': 1}, {'x': 2}), batch_metadata={'source': 'NEW_YORK_TIMES'},
             run_id='my_test_run', do_zombie_check=False)
    rows = run_and_fetchall(conn, 'SELECT * FROM tmp_scd')
    print(rows)
    is_three_rows = (len(rows) == 3)
    is_most_recent_always_true = all([row['_scd_is_most_recent'] for row in rows])
    conn.close()
    return _is_okay_invariants_scd_table(rows, False) and is_three_rows and is_most_recent_always_true


def is_scenario_36_success(database, schema, get_conn_callback) -> bool:
    conn = get_conn_callback()
    create_tables(conn, database, schema, 't', {'x': 'INT', 'y': 'INT'}, partition_columns=('x', 'y'))
    run(conn, "INSERT INTO tmp_in (t, x, y) VALUES ('20210609T050000', 1, 1), ('20210609T050000', 1, 2), ('20210609T050000', 2, 1);")
    # use partitions, two partition columns this time
    snapshot(conn, f'{database}.{schema}.tmp_batch_metadata', f'{database}.{schema}.tmp_in', f'{database}.{schema}.tmp_stage', f'{database}.{schema}.tmp_changes',
             f'{database}.{schema}.tmp_scd', 'txy', 'xy', 't', '20210609T050000', partitions=({'x': 1, 'y': 2},), batch_metadata={'source': 'WEEKLY'}, do_zombie_check=False)
    rows = run_and_fetchall(conn, 'SELECT * FROM tmp_scd')
    print(rows)
    is_one_row = (len(rows) == 1)
    is_most_recent_always_true = all([row['_scd_is_most_recent'] for row in rows])
    conn.close()
    return _is_okay_invariants_scd_table(rows, False) and is_one_row and is_most_recent_always_true


def is_scenario_37_success(database, schema, get_conn_callback) -> bool:
    conn = get_conn_callback()
    create_tables(conn, database, schema, 't_', {'x': 'INT', 'y': 'INT'}, partition_columns=('x', 'y'))
    run(conn, "INSERT INTO tmp_in (t_, x, y) VALUES ('20210609T050000', 1, 1), ('20210609T050000', 1, 2), ('20210609T050000', 2, 1);")
    # Use lists or tuples (like we're supposed to) instead of strings that Python gladly iterates over (Python is a toy language)
    # Also use a multi letter ts_nodash_col to surface a silly bug that existed once in snapshot
    snapshot(conn, f'{database}.{schema}.tmp_batch_metadata', f'{database}.{schema}.tmp_in', f'{database}.{schema}.tmp_stage', f'{database}.{schema}.tmp_changes',
             f'{database}.{schema}.tmp_scd', ('t_', 'x', 'y'), ('x', 'y'), 't_', '20210609T050000', partitions=({'x': 1, 'y': 2},), batch_metadata={'source': 'WEEKLY'}, do_zombie_check=False)
    rows = run_and_fetchall(conn, 'SELECT * FROM tmp_scd')
    print(rows)
    is_one_row = (len(rows) == 1)
    is_most_recent_always_true = all([row['_scd_is_most_recent'] for row in rows])
    conn.close()
    return _is_okay_invariants_scd_table(rows, False) and is_one_row and is_most_recent_always_true
    

def is_scenario_38_success(database, schema, get_conn_callback) -> bool:
    conn = get_conn_callback()
    # use a NOT NULL constraint in one of the columns to surface a bug we had in inserting the deletions in the changes table
    create_tables(conn, database, schema, 't', {'x': 'INT NOT NULL', 'y': 'INT'})
    # introduce a deletion in the second batch (remember, each batch is considered to be a full load)
    run(conn, "INSERT INTO tmp_in (t, x, y) VALUES ('20210609T050000', 1, 2), ('20210609T050000', 2, 1), "
              "('20210614T050000', 1, 1);")
    snapshot(conn, f'{database}.{schema}.tmp_batch_metadata', f'{database}.{schema}.tmp_in', f'{database}.{schema}.tmp_stage', f'{database}.{schema}.tmp_changes',
             f'{database}.{schema}.tmp_scd', 'txy', 'x', 't', '20210609T050000', do_zombie_check=False)
    # a second round to stage the second batch 
    snapshot(conn, f'{database}.{schema}.tmp_batch_metadata', f'{database}.{schema}.tmp_in', f'{database}.{schema}.tmp_stage', f'{database}.{schema}.tmp_changes',
             f'{database}.{schema}.tmp_scd', 'txy', 'x', 't', '20210614T050000', do_zombie_check=False)
    rows = run_and_fetchall(conn, 'SELECT * FROM tmp_scd')
    print(rows)
    is_three_rows = (len(rows) == 3)
    is_most_recent_not_always_true = not all([row['_scd_is_most_recent'] for row in rows])
    is_a_deleted_row_present = any([row['_scd_deleted_timestamp'] for row in rows])
    conn.close()
    return _is_okay_invariants_scd_table(rows, False) and is_three_rows and is_most_recent_not_always_true and is_a_deleted_row_present


def is_scenario_39_success(database, schema, get_conn_callback) -> bool:
    conn = get_conn_callback()
    create_tables(conn, database, schema, 't', {'x': 'INT', 'y': 'INT'}, partition_columns=('x',))
    # Check that a new row is only inserted for the partition it belongs to
    run(conn, "INSERT INTO tmp_in (t, x, y) VALUES ('20210609T050000', 1, 1), ('20210614T050000', 2, 2), ('20210629T050000', 2, 2), ('20210629T050000', 2, 3);")
    snapshot(conn, f'{database}.{schema}.tmp_batch_metadata', f'{database}.{schema}.tmp_in', f'{database}.{schema}.tmp_stage', f'{database}.{schema}.tmp_changes',
             f'{database}.{schema}.tmp_scd', 'txy', 'xy', 't', '20210609T050000', partitions=({'x': 1},), do_zombie_check=False)
    # a second round to stage the second batch 
    snapshot(conn, f'{database}.{schema}.tmp_batch_metadata', f'{database}.{schema}.tmp_in', f'{database}.{schema}.tmp_stage', f'{database}.{schema}.tmp_changes',
             f'{database}.{schema}.tmp_scd', 'txy', 'xy', 't', '20210614T050000', partitions=({'x': 2},), do_zombie_check=False)
    # a third round to stage the third batch 
    snapshot(conn, f'{database}.{schema}.tmp_batch_metadata', f'{database}.{schema}.tmp_in', f'{database}.{schema}.tmp_stage', f'{database}.{schema}.tmp_changes',
             f'{database}.{schema}.tmp_scd', 'txy', 'xy', 't', '20210629T050000', partitions=({'x': 2},), do_zombie_check=False)
    rows = run_and_fetchall(conn, 'SELECT * FROM tmp_scd')
    print(rows)
    is_three_rows = (len(rows) == 3)
    is_most_recent_always_true = all([row['_scd_is_most_recent'] for row in rows])
    is_deleted_always_none = all([row['_scd_deleted_timestamp'] is None for row in rows])
    conn.close()
    return _is_okay_invariants_scd_table(rows, False) and is_three_rows and is_most_recent_always_true and is_deleted_always_none


def is_scenario_40_success(database, schema, get_conn_callback) -> bool:
    conn = get_conn_callback()
    create_tables(conn, database, schema, 't', {'x': 'INT', 'y': 'INT'}, partition_columns=('x',))
    # Check that a new but unchanged row does not cause an insert of the new row + an invalidation of the old row
    run(conn, "INSERT INTO tmp_in (t, x, y) VALUES ('20210614T050000', 2, 2), ('20210629T050000', 2, 2);")
    snapshot(conn, f'{database}.{schema}.tmp_batch_metadata', f'{database}.{schema}.tmp_in', f'{database}.{schema}.tmp_stage', f'{database}.{schema}.tmp_changes',
             f'{database}.{schema}.tmp_scd', 'txy', 'xy', 't', '20210614T050000', partitions=({'x': 2},), do_zombie_check=False)
    # a second round to stage the second batch 
    snapshot(conn, f'{database}.{schema}.tmp_batch_metadata', f'{database}.{schema}.tmp_in', f'{database}.{schema}.tmp_stage', f'{database}.{schema}.tmp_changes',
             f'{database}.{schema}.tmp_scd', 'txy', 'xy', 't', '20210629T050000', partitions=({'x': 2},), do_zombie_check=False)
    rows = run_and_fetchall(conn, 'SELECT * FROM tmp_scd')
    print(rows)
    is_one_row = (len(rows) == 1)
    is_most_recent_always_true = all([row['_scd_is_most_recent'] for row in rows])
    is_deleted_always_none = all([row['_scd_deleted_timestamp'] is None for row in rows])
    conn.close()
    return _is_okay_invariants_scd_table(rows, False) and is_one_row and is_most_recent_always_true and is_deleted_always_none