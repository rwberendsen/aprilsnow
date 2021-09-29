import json
import logging

from util import run, run_and_fetchall
from util import sql_surrogate_key, sql_ts_nodash_to_timestamp_ntz
from test_unique_compound_key import test_unique_compound_key


def _get_or_clause_partition(i, partition_columns, table=None):
    table = '' if table is None else table
    return """
    (
          {or_clause:s}
    )
""".format(or_clause=f'\n      AND '.join([f"IDENTIFIER('{table}{'' if table == '' else '.'}{col}') = %(__value_partition_{i:d}_column_{col}__)s" for col in partition_columns]))


def _get_or_clauses_partitions(partitions, table=None):
    return """
  AND
  (
    {or_clauses}
  )
""".format(or_clauses='    OR'.join([_get_or_clause_partition(i, partition.keys(), table=table) for i, partition in enumerate(partitions)]))


def _get_partitions_values(partitions, partition_columns):
    return ',\n'.join(["""
(
    %(ts_nodash)s
  , {partition_values:s}
  , %(run_id)s
  , %(batch_metadata_json)s
)
""".format(partition_values='\n  , '.join(
        [f"%(__value_partition_{i:d}_column_{col}__)s" for col in partition_columns])) for i, _ in enumerate(partitions)])


def _store_batch_metadata(conn, batch_metadata_table, ts_nodash_col, ts_nodash, partitions=None, run_id=None, batch_metadata=None):
    """
    Checks if ts_nodash is indeed later than last processed ts_nodash (for all partitions); if so, it writes the row(s) stating that
    this batch has been done (we trust the user of this function to only commit once that has in fact really happened).

    Parameters
    ----------
    conn : object
        A Snowflake Python Connector database connection
    batch_metadata_table : str
    ts_nodash_col : str
        Which column in the batch_metadata_table contains the batch id timestamp (in Airflow's ts_nodash format)
    ts_nodash : str
        The value of the batch you want to stage
    partitions: list, optional
        Can be used to select only some partitions of the source table. Format: [{'column_1': value_1, ...}, ...]
    run_id : str, optional
        An identifier for the run that is invoking this function
    batch_metadata : dict, optional
        Anything you would like to add about the ts_nodash (the batch), will be
        serialized as JSON and stored batch_metadata_table._scd_batch_metadata VARIANT
    
    Notes
    -----
    Transaction control is left to the caller. All statements issued keep a transaction intact. No DDL statements are issued.
    """
    partitions = [] if partitions is None else partitions
    if partitions:
        partition_columns = partitions[0].keys()
        if ts_nodash_col in frozenset(partition_columns):
            raise ValueError('Partition columns should not include ts_nodash_col')
        for partition in partitions:
            if frozenset(partition.keys()) ^ frozenset(partition_columns):
                raise ValueError('The same partition columns should be used for each partition')
    else:
        partition_columns = []
    sql_batches_not_earlier = """
SELECT 
    IDENTIFIER(%(ts_nodash_col)s)
  {partition_columns:s}
FROM IDENTIFIER(%(batch_metadata_table)s)
WHERE 
      IDENTIFIER(%(ts_nodash_col)s) >= %(ts_nodash)s
  {partitions_clause:s}
""".format(
            partition_columns=', ' + '\n  , '.join(partition_columns) if partitions else '',
            partitions_clause = _get_or_clauses_partitions(partitions) if partitions else ''
    )
    params={'ts_nodash_col': ts_nodash_col, 'ts_nodash': ts_nodash, 'batch_metadata_table': batch_metadata_table}
    if partitions:
        params.update({f'__value_partition_{i:d}_column_{col}__': v for i, partition in enumerate(partitions) for col, v in partition.items()})
    rows = run_and_fetchall(conn, sql_batches_not_earlier, params=params)
    if rows:
        raise ValueError(f'Batches found that were no earlier than candidate batch')
    
    sql_insert = """
INSERT INTO IDENTIFIER(%(batch_metadata_table)s)
(
    {ts_nodash_col:s}
  {partition_columns:s}
  , _scd_created_by
  , _scd_batch_metadata
)
SELECT 
    column1 AS {ts_nodash_col:s}
  {column_i_as_partition_columns:s}
  , column{n_partition_columns_plus_two:d} AS _scd_created_by
  , parse_json(column{n_partition_columns_plus_three:d}) AS _scd_batch_metadata
FROM 
VALUES
{partitions_or_batch_values:s}
""".format(
            ts_nodash_col=ts_nodash_col,
            partition_columns=', ' + '\n  , '.join(partition_columns) if partitions else '',
            column_i_as_partition_columns='' if not partitions else ', ' + '\n  ,'.join([f'column{i + 2:d} AS {col}' for i, col in enumerate(partition_columns)]),
            n_partition_columns_plus_two=len(partition_columns) + 2,
            n_partition_columns_plus_three=len(partition_columns) + 3,
            partitions_or_batch_values="""
(
    %(ts_nodash)s
  , %(run_id)s
  , %(batch_metadata_json)s
)
""" if not partitions else _get_partitions_values(partitions, partition_columns)
    )
    params = {'batch_metadata_table': batch_metadata_table, 'ts_nodash': ts_nodash, 'run_id': run_id, 'batch_metadata_json': json.dumps(batch_metadata)}
    if partitions:
        params.update({f'__value_partition_{i:d}_column_{col}__': v for i, partition in enumerate(partitions) for col, v in partition.items()})
    run(conn, sql_insert, params=params)


def _stage_partition_batches(conn, source_table, staging_table, columns, unique_compound_key, ts_nodash_col, ts_nodash,
                            partitions=None, run_id=None):
    """
    Stages some data for snapshotting.

    Parameters
    ----------
    conn : object
        A Snowflake Python Connector database connection
    source_table : str
        Table you want to stage data from. Should have a column with a batch timestamp in the Airflow ts_nodash format.
    staging_table : str
    columns : list
        The columns you want to stage from the source table
    unique_compound_key : list
        A subset of columns that together form a unique compound key within a ts_nodash batch. If
        that key is violated in the staging table for the batch, all rows will be staged
        in the target table.
    ts_nodash_col : str
        Which column in the source (and target) table contains the batch id timestamp (in Airflow's ts_nodash format)
    ts_nodash : str
        The value of the batch you want to stage
    partitions: list, optional
        Can be used to select only some partitions of the source table. Format: [{'column_1': value_1, ...}, ...]
    run_id : str, optional
        An identifier for the run that is invoking this function
    
    Notes
    -----
    Transaction control is left to the caller. All statements issued keep a transaction intact. No DDL statements are issued.

    The target table is TRUNCATED before the data is staged.
    """
    if frozenset(unique_compound_key) - frozenset(columns):
        raise ValueError('unique_compound_key Should be a subset of columns')
    if ts_nodash_col not in frozenset(columns):
        raise ValueError('ts_nodash_col Should be in columns')
    if ts_nodash_col in unique_compound_key:
        raise ValueError('ts_nodash_col Should not be used as part of unique_compound_key') 
    partitions = [] if partitions is None else partitions
    if partitions:
        partition_columns = partitions[0].keys()
        if frozenset(partition_columns) - frozenset(unique_compound_key):
            raise ValueError('Partition columns should be a subset of unique compound key columns')
        for partition in partitions:
            if frozenset(partition.keys()) ^ frozenset(partition_columns):
                raise ValueError('The same partition columns should be used for each partition')
    run(conn, "TRUNCATE TABLE IDENTIFIER(%(staging_table)s);", params={'staging_table': staging_table})
    sql = """
INSERT INTO IDENTIFIER(%(staging_table)s)
(
    {column_names:s} 
  , _scd_normalised_key
  , _scd_created_by
)
SELECT
    {column_names:s}
  , {sql_scd_normalised_key:s} 
  , %(run_id)s
FROM IDENTIFIER(%(source_table)s)
WHERE
      IDENTIFIER(%(ts_nodash_col)s) = %(ts_nodash)s
{partitions_clause:s}
;
""".format(
        column_names = '\n  , '.join([f'{column:s}' for column in columns]),
        sql_scd_normalised_key=sql_surrogate_key(unique_compound_key),
        partitions_clause = _get_or_clauses_partitions(partitions) if partitions else ''
)
    params={'source_table': source_table, 'staging_table': staging_table, 'ts_nodash_col': ts_nodash_col,
            'ts_nodash': ts_nodash, 'run_id': run_id}
    if partitions:
        params.update({f'__value_partition_{i:d}_column_{col}__': v for i, partition in enumerate(partitions) for col, v in partition.items()})
    run(conn, sql, params=params)
    # TODO: consider having these functions just return the proper SQL code, rather than also running them
    # Unit test them by comparing strings somehow (or parse trees of Snowflake
    # SQL parsers ;-p)
    
 
def _sql_some_cols_differ(columns_lhs, columns_rhs):
    if not columns_lhs:
        return 'FALSE'
    return '  (\n' + '\n    OR '.join([f"NOT EQUAL_NULL(IDENTIFIER('{i[0]}'), IDENTIFIER('{i[1]}'))" for i in zip(columns_lhs, columns_rhs)]) + '\n  )'


def _stage_snapshot_changes(conn, staging_table, changes_table, scd_table, columns, unique_compound_key, ts_nodash_col, ts_nodash, partitions=None, run_id=None):
    """
    Really an intermediate step only: stages inserts, updates, and deletes for a subsequent MERGE INTO.

    Parameters
    ----------
    conn : object
        A Snowflake Python Connector database connection
    staging_table : str
        Table that contains the staged data. 
    changes_table : str
    columns : list
        The columns you want to stage from the source table: use the same set of columns you used with the function
        _stage_partition_batches
    unique_compound_key: list
    ts_nodash_col : str
        Which column in the source (and target) table contains the batch id timestamp (in Airflow's ts_nodash format)
    ts_nodash: str
    partitions: list, optional
        Should be equal to partitions used when staging rows from the source table
    run_id : str, optional
        An identifier for the (Airflow) run that is invoking this function
    
    Notes
    -----
    Transaction control is left to the caller. All statements issued keep a transaction intact. No DDL statements are issued.

    The target table is TRUNCATED before the data is staged.
    """
    if frozenset(unique_compound_key) - frozenset(columns):
        raise ValueError('unique_compound_key Should be a subset of columns')
    if ts_nodash_col not in frozenset(columns):
        raise ValueError('ts_nodash_col Should be in columns')
    if ts_nodash_col in unique_compound_key:
        raise ValueError('ts_nodash_col Should not be used as part of unique_compound_key') 
    partitions = [] if partitions is None else partitions
    if partitions:
        partition_columns = partitions[0].keys()
        if frozenset(partition_columns) - frozenset(columns):
            raise ValueError('Partition columns should be a subset of columns')
        for partition in partitions:
            if frozenset(partition.keys()) ^ frozenset(partition_columns):
                raise ValueError('The same partition columns should be used for each partition')
    non_unique_compound_key_cols = list(frozenset(columns) - frozenset(unique_compound_key) - frozenset([ts_nodash_col]))
    run(conn, 'TRUNCATE TABLE IDENTIFIER(%(changes_table)s)', params={'changes_table': changes_table})
    sql = """
INSERT INTO IDENTIFIER(%(changes_table)s) 
(
    {column_names:s}
  , _scd_normalised_key
  , _scd_id
  , _scd_valid_from_timestamp
  , _scd_valid_to_timestamp
  , _scd_deleted_timestamp
  , _scd_is_most_recent
  -- The created_ and last_modified fields correspond to the time of the DML changes being made;
  -- Merely moving an unchanged row from the SCD table and inserting it back will not alter these fields
  , _scd_created_timestamp          
  , _scd_last_modified_timestamp
  , _scd_created_by
  , _scd_last_modified_by
  , _scd_last_dml_action
  , _scd_priority  -- used for deciding which row from the changes table goes (back) in to the scd table; the altered one or the unchanged one
)

WITH _scd_active_snapshot AS
(
SELECT 
    {column_names:s}
  , _scd_normalised_key
  , _scd_id
  , _scd_valid_from_timestamp
  , _scd_valid_to_timestamp
  , _scd_deleted_timestamp
  , _scd_is_most_recent
  , _scd_created_timestamp          
  , _scd_last_modified_timestamp
  , _scd_created_by
  , _scd_last_modified_by
  , _scd_last_dml_action
FROM IDENTIFIER(%(scd_table)s)
WHERE _scd_valid_to_timestamp IS NULL
AND _scd_deleted_timestamp IS NULL
{partitions_clause:s}
),

-- When the normalised key is not really unique..., real world data is a messy business
_scd_intermediate_numbered AS
(
SELECT
    {column_names:s}
  , _scd_normalised_key
  -- Below, ORDER BY is required, but it does not matter which row we pick
  , ROW_NUMBER() OVER (PARTITION BY _scd_normalised_key ORDER BY _scd_normalised_key) AS _scd_row_number
FROM IDENTIFIER(%(staging_table)s)
-- Filtering by ts_nodash_col = ts_nodash not necessary, as there was only one batch staged anyway
),

_scd_intermediate AS
(
SELECT
    {column_names:s}
  , _scd_normalised_key
FROM _scd_intermediate_numbered
WHERE _scd_row_number = 1   
),

_scd_insert_news AS
(
SELECT
    {column_names_intermediate:s}
  , _scd_intermediate._scd_normalised_key
  , {sql_scd_id:s} AS _scd_id
  , {sql_ts_nodash_to_timestamp_ntz:s} AS _scd_valid_from_timestamp
  , NULL AS _scd_valid_to_timestamp
  , NULL AS _scd_deleted_timestamp
  , TRUE AS _scd_is_most_recent
  , SYSDATE() AS _scd_created_timestamp          
  , SYSDATE() AS _scd_last_modified_timestamp
  , %(run_id)s AS _scd_created_by
  , %(run_id)s AS _scd_last_modified_by
  , 'insert_new' AS _scd_last_dml_action
  , 1 AS _scd_priority
FROM _scd_intermediate
LEFT JOIN _scd_active_snapshot
  ON
     _scd_intermediate._scd_normalised_key = _scd_active_snapshot._scd_normalised_key
WHERE
     _scd_active_snapshot._scd_normalised_key IS NULL
),

_scd_insert_updates AS
(
SELECT
    {column_names_intermediate:s}
  , _scd_intermediate._scd_normalised_key
  , {sql_scd_id:s} AS _scd_id
  , {sql_ts_nodash_to_timestamp_ntz:s} AS _scd_valid_from_timestamp
  , NULL AS _scd_valid_to_timestamp
  , NULL AS _scd_deleted_timestamp
  , TRUE AS _scd_is_most_recent
  , SYSDATE() AS _scd_created_timestamp          
  , SYSDATE() AS _scd_last_modified_timestamp
  , %(run_id)s AS _scd_created_by
  , %(run_id)s AS _scd_last_modified_by
  , 'insert_update' AS _scd_last_dml_action
  , 1 AS _scd_priority
FROM _scd_intermediate
INNER JOIN _scd_active_snapshot
  ON
     _scd_intermediate._scd_normalised_key = _scd_active_snapshot._scd_normalised_key
WHERE
  {sql_some_cols_differ:s} 
),

_scd_invalidations AS
(
SELECT
    {column_names_active_snapshot:s}
  , _scd_active_snapshot._scd_normalised_key
  , _scd_active_snapshot._scd_id 
  , _scd_active_snapshot._scd_valid_from_timestamp
  , {sql_ts_nodash_to_timestamp_ntz:s} AS _scd_valid_to_timestamp
  , NULL AS _scd_deleted_timestamp
  , FALSE AS _scd_is_most_recent
  , _scd_active_snapshot._scd_created_timestamp          
  , SYSDATE() AS _scd_last_modified_timestamp
  , _scd_active_snapshot._scd_created_by
  , %(run_id)s AS _scd_last_modified_by
  , 'invalidate' AS _scd_last_dml_action
  , 1 AS _scd_priority
FROM _scd_intermediate
INNER JOIN _scd_active_snapshot
  ON
     _scd_intermediate._scd_normalised_key = _scd_active_snapshot._scd_normalised_key
WHERE
  {sql_some_cols_differ:s} 
),

_scd_deletes AS
(
SELECT
    {column_names_active_snapshot:s}
  , _scd_active_snapshot._scd_normalised_key
  , _scd_active_snapshot._scd_id
  , _scd_active_snapshot._scd_valid_from_timestamp
  , _scd_active_snapshot._scd_valid_to_timestamp
  , {sql_ts_nodash_to_timestamp_ntz:s} AS _scd_deleted_timestamp
  , _scd_active_snapshot._scd_is_most_recent
  , _scd_active_snapshot._scd_created_timestamp          
  , SYSDATE() AS _scd_last_modified_timestamp
  , _scd_active_snapshot._scd_created_by
  , %(run_id)s AS _scd_last_modified_by
  , 'delete' AS _scd_last_dml_action
  , 1 AS _scd_priority
FROM _scd_active_snapshot 
LEFT JOIN _scd_intermediate
  ON _scd_active_snapshot._scd_normalised_key = _scd_intermediate._scd_normalised_key
WHERE
      _scd_intermediate._scd_normalised_key IS NULL
)

SELECT * FROM _scd_insert_news
UNION ALL
SELECT * FROM _scd_insert_updates
UNION ALL
SELECT * FROM _scd_invalidations
UNION ALL
SELECT * FROM _scd_deletes
""".format(
        column_names='\n  , '.join([f'{column:s}' for column in columns]),
        partitions_clause = _get_or_clauses_partitions(partitions) if partitions else '',
        column_names_intermediate='\n  , '.join([f'_scd_intermediate.{column:s}' for column in columns]),
        column_names_active_snapshot='\n  , '.join([f'_scd_active_snapshot.{column:s}' for column in columns]),
        sql_scd_id=sql_surrogate_key([f'_scd_intermediate.{column}' for column in ('_scd_normalised_key', ts_nodash_col)]),
        sql_some_cols_differ=_sql_some_cols_differ(
                [f'_scd_intermediate.{col}' for col in non_unique_compound_key_cols],
                [f'_scd_active_snapshot.{col}' for col in non_unique_compound_key_cols]
        ),
        sql_ts_nodash_to_timestamp_ntz=sql_ts_nodash_to_timestamp_ntz('%(ts_nodash)s')
    )
    params = {'staging_table': staging_table, 'changes_table': changes_table, 'scd_table': scd_table, 'run_id': run_id, 'ts_nodash': ts_nodash}
    if partitions:
        params.update({f'__value_partition_{i:d}_column_{col}__': v for i, partition in enumerate(partitions) for col, v in partition.items()})
    run(conn, sql, params=params)


def _stage_zombie_changes(conn, changes_table, scd_table, columns, partitions=None, run_id=None):
    """
    Really an intermediate step only: stages updates for deceased rows: deleted rows that re-appeared again.

    Parameters
    ----------
    conn : object
        A Snowflake Python Connector database connection
    changes_table : str
    columns : list
        The columns you want to stage from the source table: use the same set of columns you used with the function
        _stage_partition_batches
    partitions: list, optional
        Should be equal to partitions used when staging rows from the source table
    run_id : str, optional
        An identifier for the (Airflow) run that is invoking this function
    
    Notes
    -----
    Transaction control is left to the caller. All statements issued keep a transaction intact. No DDL statements are issued.
    """
    partitions = [] if partitions is None else partitions
    if partitions:
        partition_columns = partitions[0].keys()
        if frozenset(partition_columns) - frozenset(columns):
            raise ValueError('Partition columns should be a subset of columns')
        for partition in partitions:
            if frozenset(partition.keys()) ^ frozenset(partition_columns):
                raise ValueError('The same partition columns should be used for each partition')
    sql = """
INSERT INTO IDENTIFIER(%(changes_table)s) 
(
    {column_names:s}
  , _scd_normalised_key
  , _scd_id
  , _scd_valid_from_timestamp
  , _scd_valid_to_timestamp
  , _scd_deleted_timestamp
  , _scd_is_most_recent
  , _scd_created_timestamp          
  , _scd_last_modified_timestamp
  , _scd_created_by
  , _scd_last_modified_by
  , _scd_last_dml_action
  , _scd_priority 
)

WITH _scd_graveyard AS
(
SELECT
    {column_names:s}
  , _scd_normalised_key
  , _scd_id
  , _scd_valid_from_timestamp
  , _scd_valid_to_timestamp
  , _scd_deleted_timestamp
  , _scd_is_most_recent
  , _scd_created_timestamp          
  , _scd_last_modified_timestamp
  , _scd_created_by
  , _scd_last_modified_by
  , _scd_last_dml_action
FROM IDENTIFIER(%(scd_table)s)
WHERE _scd_valid_to_timestamp IS NULL
AND _scd_deleted_timestamp IS NOT NULL
{partitions_clause:s}
),

_scd_insert_news AS
(
SELECT
    _scd_normalised_key
  , _scd_valid_from_timestamp
FROM IDENTIFIER(%(changes_table)s)
WHERE _scd_last_dml_action = 'insert_new'  -- in many cases arelatively small set
),

_scd_update_deceased AS  -- potentially expensive "zombie check", but, zombies occur in the real world ;-)
(
SELECT
    {column_names_graveyard:s}
  , _scd_graveyard._scd_normalised_key
  , _scd_graveyard._scd_id
  , _scd_graveyard._scd_valid_from_timestamp
  , _scd_insert_news._scd_valid_from_timestamp AS _scd_valid_to_timestamp  -- read this line twice :-)
  , _scd_graveyard._scd_deleted_timestamp
  , FALSE AS _scd_is_most_recent
  , _scd_graveyard._scd_created_timestamp          
  , SYSDATE() AS _scd_last_modified_timestamp
  , _scd_graveyard._scd_created_by
  , %(run_id)s AS _scd_last_modified_by
  , 'invalidate_deceased' AS _scd_last_dml_action
  , 1 AS _scd_priority
FROM _scd_insert_news  -- small set, typically
INNER JOIN _scd_graveyard -- very large set, potentially, and one that could grow over time as well
  ON _scd_insert_news._scd_normalised_key = _scd_graveyard._scd_normalised_key
)

SELECT * FROM _scd_update_deceased
""".format(
        column_names='\n  , '.join([f'{column:s}' for column in columns]),
        partitions_clause=_get_or_clauses_partitions(partitions) if partitions else '',
        column_names_graveyard='\n  , '.join([f'_scd_graveyard.{column:s}' for column in columns])
    )
    params = {'changes_table': changes_table, 'scd_table': scd_table, 'run_id': run_id}
    if partitions:
        params.update({f'__value_partition_{i:d}_column_{col}__': v for i, partition in enumerate(partitions) for col, v in partition.items()})
    run(conn, sql, params=params)


def _stage_most_recent_part_of_partitions(conn, changes_table, scd_table, columns, do_zombie_check, partitions=None):
    """
    Really an intermediate step only: stages basically the valid part of the partitions in the changes table as well.

    Parameters
    ----------
    conn : object
        A Snowflake Python Connector database connection
    changes_table : str
    columns : list
        The columns you want to stage from the source table: use the same set of columns you used with the function
        _stage_partition_batches
    do_zombie_check : boolean
        If we did not do a zombie check, there is no need to stage unchanged deleted rows (and also no need to delete them later)
    partitions: list, optional
        Should be equal to partitions used when staging rows from the source table
    
    Notes
    -----
    Transaction control is left to the caller. All statements issued keep a transaction intact. No DDL statements are issued.
    """
    partitions = [] if partitions is None else partitions
    if partitions:
        partition_columns = partitions[0].keys()
        if frozenset(partition_columns) - frozenset(columns):
            raise ValueError('Partition columns should be a subset of columns')
        for partition in partitions:
            if frozenset(partition.keys()) ^ frozenset(partition_columns):
                raise ValueError('The same partition columns should be used for each partition')
    sql = """
INSERT INTO IDENTIFIER(%(changes_table)s) 
(
    {column_names:s}
  , _scd_normalised_key
  , _scd_id
  , _scd_valid_from_timestamp
  , _scd_valid_to_timestamp
  , _scd_deleted_timestamp
  , _scd_is_most_recent
  , _scd_created_timestamp          
  , _scd_last_modified_timestamp
  , _scd_created_by
  , _scd_last_modified_by
  , _scd_last_dml_action
  , _scd_priority 
)

SELECT
    {column_names:s}
  , _scd_normalised_key
  , _scd_id
  , _scd_valid_from_timestamp
  , _scd_valid_to_timestamp
  , _scd_deleted_timestamp
  , _scd_is_most_recent
  , _scd_created_timestamp          
  , _scd_last_modified_timestamp
  , _scd_created_by
  , _scd_last_modified_by
  , _scd_last_dml_action
  , 2 AS _scd_priority  -- lower priority than 1
FROM IDENTIFIER(%(scd_table)s)
WHERE _scd_valid_to_timestamp IS NULL
{sql_zombie_check:s}
{partitions_clause:s}
""".format(
        column_names='\n  , '.join([f'{column:s}' for column in columns]),
        sql_zombie_check='' if do_zombie_check else 'AND _scd_deleted_timestamp IS NULL',
        partitions_clause=_get_or_clauses_partitions(partitions) if partitions else ''
    )
    params = {'changes_table': changes_table, 'scd_table': scd_table}
    if partitions:
        params.update({f'__value_partition_{i:d}_column_{col}__': v for i, partition in enumerate(partitions) for col, v in partition.items()})
    run(conn, sql, params=params)


def _delete_most_recent_part_of_partitions(conn, scd_table, do_zombie_check, partitions=None):
    """
    Really an intermediate step only: delete the valid part of the partitions in the scd table.

    Parameters
    ----------
    conn : object
        A Snowflake Python Connector database connection
    scd_table : str
    do_zombie_check : boolean
        If we did not do a zombie check, there is no need to stage unchanged deleted rows (and also no need to delete them later)
    partitions: list, optional
        Should be equal to partitions used when staging rows from the source table
    
    Notes
    -----
    Transaction control is left to the caller. All statements issued keep a transaction intact. No DDL statements are issued.
    """
    partitions = [] if partitions is None else partitions
    if partitions:
        partition_columns = partitions[0].keys()
        for partition in partitions:
            if frozenset(partition.keys()) ^ frozenset(partition_columns):
                raise ValueError('The same partition columns should be used for each partition')
    sql = """
DELETE FROM IDENTIFIER(%(scd_table)s) 
WHERE _scd_valid_to_timestamp IS NULL
{sql_zombie_check:s}
{partitions_clause:s}
""".format(
        sql_zombie_check='' if do_zombie_check else 'AND _scd_deleted_timestamp IS NULL',
        partitions_clause=_get_or_clauses_partitions(partitions) if partitions else ''
    )
    params = {'scd_table': scd_table}
    if partitions:
        params.update({f'__value_partition_{i:d}_column_{col}__': v for i, partition in enumerate(partitions) for col, v in partition.items()})
    run(conn, sql, params=params)


def _insert_changed_and_unchanged(conn, changes_table, scd_table, columns):
    """
    Final step: insert changed and unchanged rows from changes table, favouring changed rows

    Parameters
    ----------
    conn : object
        A Snowflake Python Connector database connection
    changes_table : str
    scd_table : str
    columns : list
    
    Notes
    -----
    Transaction control is left to the caller. All statements issued keep a transaction intact. No DDL statements are issued.
    """
    sql = """
INSERT INTO IDENTIFIER(%(scd_table)s) 
(
    {column_names:s}
  , _scd_normalised_key
  , _scd_id
  , _scd_valid_from_timestamp
  , _scd_valid_to_timestamp
  , _scd_deleted_timestamp
  , _scd_is_most_recent
  , _scd_created_timestamp          
  , _scd_last_modified_timestamp
  , _scd_created_by
  , _scd_last_modified_by
  , _scd_last_dml_action
)

WITH _ordered_by_priority AS
(
SELECT
    {column_names:s}
  , _scd_normalised_key
  , _scd_id
  , _scd_valid_from_timestamp
  , _scd_valid_to_timestamp
  , _scd_deleted_timestamp
  , _scd_is_most_recent
  , _scd_created_timestamp          
  , _scd_last_modified_timestamp
  , _scd_created_by
  , _scd_last_modified_by
  , _scd_last_dml_action
  , ROW_NUMBER() OVER (PARTITION BY _scd_id ORDER BY _scd_priority ASC) AS rn 
FROM IDENTIFIER(%(changes_table)s)
)

SELECT
    {column_names:s}
  , _scd_normalised_key
  , _scd_id
  , _scd_valid_from_timestamp
  , _scd_valid_to_timestamp
  , _scd_deleted_timestamp
  , _scd_is_most_recent
  , _scd_created_timestamp          
  , _scd_last_modified_timestamp
  , _scd_created_by
  , _scd_last_modified_by
  , _scd_last_dml_action
FROM _ordered_by_priority
WHERE rn = 1
""".format(
        column_names='\n  , '.join([f'{column:s}' for column in columns])
    )
    params = {'changes_table': changes_table, 'scd_table': scd_table}
    run(conn, sql, params=params)


def snapshot(conn, batch_metadata_table, source_table, staging_table, changes_table, scd_table, columns, unique_compound_key, ts_nodash_col, ts_nodash,
             partitions=None, fail_on_duplicates=True, run_id=None, batch_metadata=None, do_zombie_check=True):
    """
    Applies a full load on a slowly changing dimensions type II table

    Parameters
    ----------
    conn : object
        A Snowflake Python Connector database connection
    source_table : str
        Table you want to stage data from. Should have a column with a batch timestamp in the Airflow ts_nodash format.
    batch_metadata_table : str
        Table you want to store batch metadata in. Should have the following
        columns:

        - <ts_nodash_col> VARCHAR
        - scd_created_by VARCHAR
        - scd_batch_metadata VARIANT
        [ - partition_column_1 <TYPE> ...]

        Notes:
        - <ts_nodash_col> is whatever the column in your source table is called that has a batch ID (formatted as Airflow's ts_nodash)
        - The unique compound key of this table will be ts_nodash_col + partition columns (if any)
    staging_table : str
        Table you want to stage data into. Should have the same columns as the columns you want to use from the source table, 
        in addition to the following columns: 

        - _scd_normalised_key VARCHAR
        - _scd_created_by VARCHAR
    changes_table : str
        Table you want to stage data the inserts, updates and deletes into. 
        Should have the same columns the as the staging table, 
        in addition to the following columns: 

        - _scd_normalised_key VARCHAR
        - _scd_id VARCHAR
        - _scd_valid_from_timestamp TIMESTAMP_NTZ
        - _scd_valid_to_timestamp TIMESTAMP_NTZ
        - _scd_deleted_timestamp TIMESTAMP_NTZ
        - _scd_is_most_recent BOOLEAN
        - _scd_created_timestamp TIMESTAMP_NTZ
        - _scd_last_modified_timestamp TIMESTAMP_NTZ
        - _scd_created_by VARCHAR
        - _scd_last_modified_by VARCHAR
        - _scd_last_dml_action VARCHAR
        - _scd_priority INT
    scd_table : str
        Your SCD Type II table.
        Should have the same columns as the staging table, 
        in addition to the following columns: 

        - _scd_normalised_key VARCHAR
        - _scd_id VARCHAR
        - _scd_valid_from_timestamp TIMESTAMP_NTZ
        - _scd_valid_to_timestamp TIMESTAMP_NTZ
        - _scd_deleted_timestamp TIMESTAMP_NTZ
        - _scd_is_most_recent BOOLEAN
        - _scd_created_timestamp TIMESTAMP_NTZ
        - _scd_last_modified_timestamp TIMESTAMP_NTZ
        - _scd_created_by VARCHAR
        - _scd_last_modified_by VARCHAR
        - _scd_last_dml_action VARCHAR
    columns : list
        The columns you want to stage from the source table.
        
        NB: do not include metadata columns such as _extract_timestamp, because
        these could make two otherwise identical rows with the same unique
        compound key appear different
    unique_compound_key : list
        A subset of columns that together form a unique compound key within a ts_nodash batch. If
        that key is violated in the staging table for the batch, the behavior of this function depends on the setting
        of fail_on_duplicates.
    ts_nodash_col : str
        Which column in the source, staging, changes and scd tables contains the batch id timestamp (in Airflow's ts_nodash format)
    ts_nodash : str
        The value of the batch you want to stage, in Airflow's ts_nodash format
    partitions: list, optional
        Can be used to select only some partitions of the source table. Format: [{'column_1': value_1, ...}, ...]
    fail_on_duplicates: boolean
        If True, then raise a ValueError if the unique compound key is violated in the source table.
        If False, then pick an arbitrary row. When using this setting it is recommended to set unique_compound_key to the same value
        as columns (i.e., use all columns you are interested in from the source table as a unique key). That way, only truly duplicate
        rows will be deduplicated.
    run_id : str, optional
        An identifier for the run that is invoking this function
    batch_metadata : dict, optional
        Anything you would like to add about the ts_nodash (the batch), will be
        serialized as JSON and stored batch_metadata_table._scd_batch_metadata VARIANT
    do_zombie_check : boolean
        If True (default) then it is handled correctly if rows disappear from a full load, only to reappear again later.
        Computationally this is somewhat expensive, so you can disable. But if you do so and it does happen you will have multiple
        valid rows for a single normalised key as a result.
    
    Notes
    -----
    Consecutive full loads can be applied to a slowly changing dimensions (SCD)
    type II table by calling this function multiple times.  One addition to SCD
    II that is made here is an additional column _scd_deleted_timestamp. This is
    used to mark rows as deleted.  The SCD II table holds all the unique
    compound key rows that were ever present in the union of all full loads. The
    last known version of each unique row is the most recent, even if it is deleted. A
    row can be un-deleted: if a unique key re-appears in a later full load, a
    new row is added to the SCD II table.

    If your source table has distinct conceptual partitions, you can select any
    number of them, and the set of partitions you select will be treated as the
    full load. Only the corresponding partitions in the SCD II table will be
    targeted for updates, deletes and inserts.

    Transaction control is left to the caller. All statements issued keep a
    transaction intact. No DDL statements are issued, because issuing DDL
    statements in Snowflake commits any active transaction.

    Because you control transactions you can snapshot (the same partitions for) several
    tables in one transaction. This has the benefit that referential integrity between
    those tables is guaranteed at all times for the consumers of these SCD II tables.
    """
    if frozenset(unique_compound_key) - frozenset(columns):
        raise ValueError('unique_compound_key Should be a subset of columns')
    if ts_nodash_col not in frozenset(columns):
        raise ValueError('ts_nodash_col Should be in columns')
    if ts_nodash_col in unique_compound_key:
        raise ValueError('ts_nodash_col Should not be used as part of unique_compound_key') 
    partitions = [] if partitions is None else partitions
    if partitions:
        partition_columns = partitions[0].keys()
        if frozenset(partition_columns) - frozenset(unique_compound_key):
            raise ValueError('Partition columns should be a subset of unique compound key columns')
        for partition in partitions:
            if frozenset(partition.keys()) ^ frozenset(partition_columns):
                raise ValueError('The same partition columns should be used for each partition')
    if len(partitions) > 1:
        # While the underlying functions generate correct SQL for multiple partitions at once,
        # some of the joins become unneccessarily large when doing multiple partitions
        # Hence, we run them one at a time.
        for partition in partitions:
            snapshot(conn, batch_metadata_table, source_table, staging_table, changes_table, scd_table, columns, unique_compound_key, ts_nodash_col, ts_nodash,
                     partitions=[partition], fail_on_duplicates=fail_on_duplicates, run_id=run_id, batch_metadata=batch_metadata, do_zombie_check=do_zombie_check)
    else: 
        # When we are here, we have only 1, or 0, partitions
        # NB! the code assumes that all helper functions below work on the very same partitions
        _store_batch_metadata(conn, batch_metadata_table, ts_nodash_col, ts_nodash, partitions=partitions, run_id=run_id, batch_metadata=batch_metadata)
        _stage_partition_batches(conn, source_table, staging_table, columns, unique_compound_key, ts_nodash_col, ts_nodash, partitions=partitions, run_id=run_id)
        if fail_on_duplicates:
            is_key_unique = test_unique_compound_key(conn, staging_table, ('_scd_normalised_key',))
            if not is_key_unique:
                msg = f'_scd_normalised_key not unique in table {staging_table}'
                raise ValueError(msg)

        # stage changes, with priority 1
        _stage_snapshot_changes(conn, staging_table, changes_table, scd_table, columns, unique_compound_key, ts_nodash_col, ts_nodash, partitions=partitions, run_id=run_id)
        if do_zombie_check:
            _stage_zombie_changes(conn, changes_table, scd_table, columns, partitions=partitions, run_id=run_id)

        # stage most recent part of target partitions also in changes table, with priority 2
        _stage_most_recent_part_of_partitions(conn, changes_table, scd_table, columns, do_zombie_check, partitions=partitions)

        # delete most recent part of target partitions from target table
        _delete_most_recent_part_of_partitions(conn, scd_table, do_zombie_check, partitions=partitions)

        # insert highest priority row per unique key (with lowest priority number) into target table from changes table
        _insert_changed_and_unchanged(conn, changes_table, scd_table, columns)