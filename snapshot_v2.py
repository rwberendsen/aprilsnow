"""
Consecutive full loads can be applied to a kind of slowly changing dimensions
(SCD) type II table using this module. One addition to SCD II that is made here
is an additional column _scd_deleted_timestamp. This is used to mark rows as
deleted. The SCD II table holds all the unique compound key rows that were ever
present in the union of all full loads. The last known version of each unique
row is the most recent, even if it is deleted. A row can be un-deleted: if a
unique key re-appears in a later full load, a new row is added to the SCD II
table.

Before you start using this module, you should create:
- Your input table
- Your SCD target table 
- Your batch metadata target table

Look at the test suite for details on which columns should be in each of these
tables.

You can use this module in two ways: sequentail execution, or parallel execution
of multiple partitions.

For the sequential execution, use the snapshot function, check the test suite for
examples on how to call it.

For parallel exection, implement the same sequence used in the snapshot function
yourself. Instead of iterating over the partitions sequentially, you would execute
them in parallel somehow (we used an Airflow dag for this, but there are probably
more suitable means of parallelisation). When you do this, take not to exceed
parallelism of twenty per table: Snowflake does not allow more than twenty 
concurrent INSERT requests to be running on the same table.

Finally, take care never to run different batches in parallel, by nature, the
idea of SCD Type II is two apply these full loads sequentially.

For consumers of the SCD table:
If you consume multiple tables that were executed for the same batches with this
module, and iyour model relies on eferential integrity, then you are in a tight
spot, because DDL statements execute in their own transaction. The best way I
see to mitigate this is for you to clone all tables to a working schema of your
own, where they will not be touched. Then, run a few checks:

    - Do all batch metadata have the same batches in them?
    - Are all the distinct run id's in _scd_last_modified_by (and
      _scd_las_created_by) in each SCD table also present in the batch metadata
      table?

If the answer to both of these questions is yes, then you should be able to
rely on refential integrity.

If the answer is no, try again to clone everything (there should normally only
be seconds between the availability of the different tables).

If you don't want to rely on referential integrity, then you should be aware
that if you use an INNER JOIN on a foreign key, you could be missing out on
a lot of rows, if one of the tables was modified and the other was not.
"""
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
    """.format(or_clause=f'\n          AND '.join([f"IDENTIFIER('{table}{'' if table == '' else '.'}{col}') = %(__value_partition_{i:d}_column_{col}__)s" for col in partition_columns]))


def _get_or_clauses_partitions(partitions, table=None):
    return """
    AND
    (
        {or_clauses}
    )
    """.format(or_clauses='        OR'.join([_get_or_clause_partition(i, partition.keys(), table=table) for i, partition in enumerate(partitions)]))


def _get_partitions_values(partitions, partition_columns):
    return ',\n'.join(["""
    (
        %(ts_nodash)s
      , {partition_values:s}
      , %(source_table)s
      , %(run_id)s
      , %(batch_metadata_json)s
    )
    """.format(partition_values='\n      , '.join(
        [f"%(__value_partition_{i:d}_column_{col}__)s" for col in partition_columns])) for i, _ in enumerate(partitions)])


def _store_batch_metadata(conn, batch_metadata_table, todo_batch_metadata_table, ts_nodash_col, ts_nodash, source_table, partitions=None, run_id=None, batch_metadata=None):
    """
    Checks if ts_nodash is indeed later than last processed ts_nodash (for all partitions); if so, it writes the row(s) to todo_batch_metadata_table 

    Parameters
    ----------
    conn : object
        A Snowflake Python Connector database connection
    batch_metadata_table : str
    todo_batch_metadata_table : str
        Table to write the batches to execute to
    ts_nodash_col : str
        Which column in the batch_metadata_table contains the batch id timestamp (in Airflow's ts_nodash format)
    ts_nodash : str
        The value of the batch you want to stage
    source_table : str
        Fully qualified name for source table for this batch.
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
            partition_columns=', ' + '\n      , '.join(partition_columns) if partitions else '',
            partitions_clause = _get_or_clauses_partitions(partitions) if partitions else ''
    )
    params={'ts_nodash_col': ts_nodash_col, 'ts_nodash': ts_nodash, 'batch_metadata_table': batch_metadata_table}
    if partitions:
        params.update({f'__value_partition_{i:d}_column_{col}__': v for i, partition in enumerate(partitions) for col, v in partition.items()})
    rows = run_and_fetchall(conn, sql_batches_not_earlier, params=params)
    if rows:
        raise ValueError(f'Batches found that were no earlier than candidate batch')
    
    sql_insert = """
    INSERT INTO IDENTIFIER(%(todo_batch_metadata_table)s)
    (
        {ts_nodash_col:s}
      {partition_columns:s}
      , _scd_source_table
      , _scd_created_by
      , _scd_batch_metadata
    )
    SELECT 
        column1 AS {ts_nodash_col:s}
      {column_i_as_partition_columns:s}
      , column{n_partition_columns_plus_two:d} AS _scd_source_table
      , column{n_partition_columns_plus_three:d} AS _scd_created_by
      , parse_json(column{n_partition_columns_plus_four:d}) AS _scd_batch_metadata
    FROM 
    VALUES
    {partitions_or_batch_values:s}
    """.format(
            ts_nodash_col=ts_nodash_col,
            partition_columns=', ' + '\n      , '.join(partition_columns) if partitions else '',
            column_i_as_partition_columns='' if not partitions else ', ' + '\n      ,'.join([f'column{i + 2:d} AS {col}' for i, col in enumerate(partition_columns)]),
            n_partition_columns_plus_two=len(partition_columns) + 2,
            n_partition_columns_plus_three=len(partition_columns) + 3,
            n_partition_columns_plus_four=len(partition_columns) + 4,
            partitions_or_batch_values="""
        (
            %(ts_nodash)s
          , %(source_table)s
          , %(run_id)s
          , %(batch_metadata_json)s
        )
        """ if not partitions else _get_partitions_values(partitions, partition_columns)
    )
    params = {'todo_batch_metadata_table': todo_batch_metadata_table, 'ts_nodash': ts_nodash, 'source_table': source_table, 'run_id': run_id, 'batch_metadata_json': json.dumps(batch_metadata)}
    if partitions:
        params.update({f'__value_partition_{i:d}_column_{col}__': v for i, partition in enumerate(partitions) for col, v in partition.items()})
    run(conn, sql_insert, params=params)


def _stage_partition_batches(conn, source_table, staging_table, columns, unique_compound_key, ts_nodash_col, ts_nodash, partitions=None, run_id=None):
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
        column_names = '\n      , '.join([f'{column:s}' for column in columns]),
        sql_scd_normalised_key=sql_surrogate_key(unique_compound_key),
        partitions_clause = _get_or_clauses_partitions(partitions) if partitions else ''
    )
    params={'source_table': source_table, 'staging_table': staging_table, 'ts_nodash_col': ts_nodash_col,
            'ts_nodash': ts_nodash, 'run_id': run_id}
    if partitions:
        params.update({f'__value_partition_{i:d}_column_{col}__': v for i, partition in enumerate(partitions) for col, v in partition.items()})
    run(conn, sql, params=params)
    
 
def _sql_some_cols_differ(columns_lhs, columns_rhs):
    if not columns_lhs:
        return '      FALSE'
    return '  (\n' + '\n        OR '.join([f"NOT EQUAL_NULL(IDENTIFIER('{i[0]}'), IDENTIFIER('{i[1]}'))" for i in zip(columns_lhs, columns_rhs)]) + '\n  )'


def _stage_snapshot_changes(conn, staging_table, changes_table, scd_table, columns, unique_compound_key, ts_nodash_col, ts_nodash, partitions=None, run_id=None,
                            additional_join_columns=None):
    """
    Really an intermediate step only: stages inserts, updates, and deletes for a subsequent MERGE INTO.

    Parameters
    ----------
    conn : object
        A Snowflake Python Connector database connection
    staging_table : str
        Table that contains the staged data. 
    changes_table : str
    scd_table : str
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
    additional_join_columns : iterable, optional
    
    Notes
    -----
    Transaction control is left to the caller. All statements issued keep a transaction intact. No DDL statements are issued.

    TODO: a further optimization could be to write a single FULL OUTER JOIN here, and produce output rows with two sets of
    columns: old and new. The fact whether or not some cols differ could move to the SELECT clause, and rows with no
    differences could be included in the output as well. This way, the later step we do now where we partition by _scd_id
    and get the row with the highest priority could be omitted. A trade-off is a lot of data movement: unchanged are moved
    twice in this scheme instead of once.
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
    FROM _scd_intermediate
    LEFT JOIN _scd_active_snapshot
    ON
          _scd_intermediate._scd_normalised_key = _scd_active_snapshot._scd_normalised_key
      {sql_additional_join_columns:s}
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
    FROM _scd_intermediate
    INNER JOIN _scd_active_snapshot
    ON
          _scd_intermediate._scd_normalised_key = _scd_active_snapshot._scd_normalised_key
      {sql_additional_join_columns:s}
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
    FROM _scd_intermediate
    INNER JOIN _scd_active_snapshot
    ON
          _scd_intermediate._scd_normalised_key = _scd_active_snapshot._scd_normalised_key
      {sql_additional_join_columns:s}
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
    FROM _scd_active_snapshot 
    LEFT JOIN _scd_intermediate
    ON
          _scd_active_snapshot._scd_normalised_key = _scd_intermediate._scd_normalised_key
      {sql_additional_join_columns:s}
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
        column_names='\n      , '.join([f'{column:s}' for column in columns]),
        partitions_clause = _get_or_clauses_partitions(partitions) if partitions else '',
        column_names_intermediate='\n      , '.join([f'_scd_intermediate.{column:s}' for column in columns]),
        column_names_active_snapshot='\n      , '.join([f'_scd_active_snapshot.{column:s}' for column in columns]),
        sql_scd_id=sql_surrogate_key([f'_scd_intermediate.{column}' for column in ('_scd_normalised_key', ts_nodash_col)]),
        sql_some_cols_differ=_sql_some_cols_differ(
                [f'_scd_intermediate.{col}' for col in non_unique_compound_key_cols],
                [f'_scd_active_snapshot.{col}' for col in non_unique_compound_key_cols]
        ),
        sql_ts_nodash_to_timestamp_ntz=sql_ts_nodash_to_timestamp_ntz('%(ts_nodash)s'),
        sql_additional_join_columns='' if additional_join_columns is None else 'AND ' + '\n      AND '.join(
                [f'_scd_active_snapshot.{col} = _scd_intermediate.{col}' for col in additional_join_columns])
    )
    params = {'staging_table': staging_table, 'changes_table': changes_table, 'scd_table': scd_table, 'run_id': run_id, 'ts_nodash': ts_nodash}
    if partitions:
        params.update({f'__value_partition_{i:d}_column_{col}__': v for i, partition in enumerate(partitions) for col, v in partition.items()})
    run(conn, sql, params=params)


def _stage_zombie_changes(conn, changes_table, scd_table, columns, partitions=None, run_id=None, additional_join_columns=None):
    """
    Really an intermediate step only: stages updates for deceased rows: deleted rows that re-appeared again.

    Parameters
    ----------
    conn : object
        A Snowflake Python Connector database connection
    changes_table : str
    scd_table : str
    columns : list
        The columns you want to stage from the source table: use the same set of columns you used with the function
        _stage_partition_batches
    partitions: list, optional
        Should be equal to partitions used when staging rows from the source table
    run_id : str, optional
        An identifier for the (Airflow) run that is invoking this function
    additional_join_columns : iterable, optional
    
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
      {additional_join_columns:s} 
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
    FROM _scd_insert_news  -- small set, typically
    INNER JOIN _scd_graveyard -- very large set, potentially, and one that could grow over time as well
      ON
            _scd_insert_news._scd_normalised_key = _scd_graveyard._scd_normalised_key
        {sql_additional_join_columns:s}
    )

    SELECT * FROM _scd_update_deceased
    """.format(
        column_names='\n      , '.join([f'{column:s}' for column in columns]),
        partitions_clause=_get_or_clauses_partitions(partitions) if partitions else '',
        column_names_graveyard='\n      , '.join([f'_scd_graveyard.{column:s}' for column in columns]),
        additional_join_columns='' if additional_join_columns is None else '      , ' + '\n      , '.join(additional_join_columns),
        sql_additional_join_columns='' if additional_join_columns is None else 'AND ' + '\n      AND '.join(
                [f'_scd_insert_news.{col} = _scd_graveyard.{col}' for col in additional_join_columns])
    )
    params = {'changes_table': changes_table, 'scd_table': scd_table, 'run_id': run_id}
    if partitions:
        params.update({f'__value_partition_{i:d}_column_{col}__': v for i, partition in enumerate(partitions) for col, v in partition.items()})
    run(conn, sql, params=params)


def _insert_changed_and_unchanged(conn, changes_table, scd_table, scd_table_clone, columns, do_zombie_check, partitions=None, additional_join_columns=None):
    """
    Final step: insert changed and unchanged rows from changes table, favouring changed rows

    Parameters
    ----------
    conn : object
        A Snowflake Python Connector database connection
    changes_table : str
    scd_table : str
    scd_table_clone : str
    columns : list
    do_zombie_check : boolean
    partitions : list, optional
    additional_join_columns : iterable, optional
    
    Notes
    -----
    Transaction control is left to the caller. All statements issued keep a transaction intact. No DDL statements are issued.

    The additional join columns, when added, just might speed up the partition
    by clause, because the query optimizer could use metadata to determine sets
    of micropartitions that need to sorted in order to have all the rows for
    particular value ranges of the additional join columns.
    """
    sql = """
    INSERT INTO IDENTIFIER(%(scd_table_clone)s) 
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

    WITH changed_unchanged AS
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
    , 1 AS _scd_priority 
    FROM IDENTIFIER(%(changes_table)s)

    UNION ALL 

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
    ),

    _ordered_by_priority AS
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
    , ROW_NUMBER() OVER (PARTITION BY {sql_additional_join_columns:s} _scd_id ORDER BY _scd_priority ASC) AS rn 
    FROM changed_unchanged
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
        column_names='\n      , '.join([f'{column:s}' for column in columns]),
        sql_zombie_check='' if do_zombie_check else '      AND _scd_deleted_timestamp IS NULL',
        partitions_clause=_get_or_clauses_partitions(partitions) if partitions else '',
        sql_additional_join_columns='' if additional_join_columns is None else ', '.join(additional_join_columns) + ', '
    )
    params = {'changes_table': changes_table, 'scd_table_clone': scd_table_clone, 'scd_table': scd_table}
    if partitions:
        params.update({f'__value_partition_{i:d}_column_{col}__': v for i, partition in enumerate(partitions) for col, v in partition.items()})
    run(conn, sql, params=params)


def _delete_todo_partitions_from_clone(conn, scd_table_clone, do_zombie_check, partitions=None):
    """
    Delete active (and valid) part of todo partitions from clone

    Parameters
    ----------
    conn : object
        A Snowflake Python Connector database connection
    scd_table_clone : str
    do_zombie_check : boolean
        If we do a zombie check, we delete the entire valid part, else, only the active part
    partitions: list, optional
    
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
    DELETE FROM IDENTIFIER(%(scd_table_clone)s) 
    WHERE 
        _scd_valid_to_timestamp IS NULL
    {sql_zombie_check:s}
    {partitions_clause:s}
    """.format(
        sql_zombie_check='' if do_zombie_check else '      AND _scd_deleted_timestamp IS NULL',
        partitions_clause=_get_or_clauses_partitions(partitions) if partitions else ''
    )
    params = {'scd_table_clone': scd_table_clone}
    if partitions:
        params.update({f'__value_partition_{i:d}_column_{col}__': v for i, partition in enumerate(partitions) for col, v in partition.items()})
    run(conn, sql, params=params)


def snapshot_partition(get_conn_callback, working_schema, table_name, scd_table, columns, unique_compound_key, ts_nodash_col,
                       partition=None, fail_on_duplicates=True, run_id=None, do_zombie_check=True, additional_join_columns=None):
    """
    Applies a full load on a slowly changing dimensions type II table

    Parameters
    ----------
    get_conn_callback : object
        A function that can be called without arguments to get a Snowflake Python Connector database connection
    working_schema : str
        Fully qualified schema name for the schema that can be used to create
        working objects; 
    table_name : str
        A conceptual table name, used to name some objects in working schema uniquely
    scd_table : str
        Your SCD Type II table.
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
    partition : dict, optional
        Can be used to select only a partition of the source table. Format: {col1: val1, ...}
    fail_on_duplicates: boolean
        If True, then raise a ValueError if the unique compound key is violated in the source table.
        If False, then pick an arbitrary row. When using this setting it is recommended to set unique_compound_key to the same value
        as columns (i.e., use all columns you are interested in from the source table as a unique key). That way, only truly duplicate
        rows will be deduplicated.
    run_id : str, optional
        An identifier for the run that is invoking this function
    do_zombie_check : boolean
        If True (default) then it is handled correctly if rows disappear from a full load, only to reappear again later.
        Computationally this is somewhat expensive, so you can disable. But if you do so and it does happen you will have multiple
        valid rows for a single normalised key as a result.
    additional_join_columns : boolean, optional
    
    Notes
    -----
    Creates a bunch of temporary tables.

    What ts-nodash to do is obtained from the data layer itself, in the todd_batch_metadata_table. This allows various
    task instances in a workflow to communicate with each other through the data layer on which they operate. This way,
    you don't have to rely on something like XCom in Airflow.
    """
    if frozenset(unique_compound_key) - frozenset(columns):
        raise ValueError('unique_compound_key Should be a subset of columns')
    if additional_join_columns is not None and frozenset(additional_join_columns) - frozenset(unique_compound_key):
        raise ValueError('additional_join_columns Should be a subset of unique_compound key')
    if ts_nodash_col not in frozenset(columns):
        raise ValueError('ts_nodash_col Should be in columns')
    if ts_nodash_col in unique_compound_key:
        raise ValueError('ts_nodash_col Should not be used as part of unique_compound_key') 

    # all helper functions actually support multiple partitions, but using them in that way is a waste of compute, making the joins unneccesarily large
    partitions = [] if partition is None else [partition]  
    if partitions:
        partition_columns = partitions[0].keys()
        if frozenset(partition_columns) - frozenset(unique_compound_key):
            raise ValueError('Partition columns should be a subset of unique compound key columns')

    # Some names
    scd_table_clone = f'{working_schema}._scd_clone_{table_name}'
    todo_batch_metadata_table = f'{working_schema}._scd_todo_batch_metadata_{table_name}'
    batch_metadata_table_clone = f'{working_schema}._scd_batch_metadata_clone_{table_name}'
    staging_table = f'{working_schema}._scd_staging_{table_name}'
    changes_table = f'{working_schema}._scd_changes_{table_name}'
            
    conn = get_conn_callback()


    # Create temp changes table from scd table clone
    run(conn, 'CREATE TEMPORARY TABLE IDENTIFIER(%(changes_table)s) LIKE IDENTIFIER(%(scd_table_clone)s);',
        params={'changes_table': changes_table, 'scd_table_clone': scd_table_clone})

    ###############################
    run(conn, 'BEGIN TRANSACTION;')
    ###############################

    # check if there is anything to do; if not, never mind
    params = {'todo_batch_metadata_table': todo_batch_metadata_table, 'batch_metadata_table_clone': batch_metadata_table_clone,
              'ts_nodash_col_todo': f'todo.{ts_nodash_col}', 'ts_nodash_col_done': f'done.{ts_nodash_col}'}
    if partition is not None:
        params.update({f'todo_{col}': f'todo.{col}' for col in partition.keys()})
        params.update({f'done_{col}': f'done.{col}' for col in partition.keys()})
        params.update({f'__value_partition_0_column_{col}__': v for col, v in partition.items()})
    todo_rows = run_and_fetchall(conn, """
    SELECT
        IDENTIFIER(%(ts_nodash_col_todo)s) AS ts_nodash
      , todo._scd_source_table AS _scd_source_table
    FROM IDENTIFIER(%(todo_batch_metadata_table)s) AS todo
    LEFT JOIN IDENTIFIER(%(batch_metadata_table_clone)s) AS done
    ON
          IDENTIFIER(%(ts_nodash_col_todo)s) = IDENTIFIER(%(ts_nodash_col_done)s)
      {partition_clause_join}
    WHERE
          IDENTIFIER(%(ts_nodash_col_done)s) IS NULL
      {partitions_clause:s}
    ;
    """.format(
            partitions_clause = _get_or_clauses_partitions(partitions, table='todo') if partitions else '',
            partition_clause_join = 'AND ' + '\n      AND '.join([f'IDENTIFIER(%(todo_{col})s) = IDENTIFIER(%(done_{col})s)' for col in partition.keys()]) \
                                    if partition is not None else ''
    ), params=params)

    if len(todo_rows) > 1:
        raise ValueError(f'More than one row found in todo batch metadata table for batch id {ts_nodash} and partition {partition}')
    if len(todo_rows) == 0:
        # This is part of expected operation, if for a ts_nodash we do not want all partitions, e.g., when backfilling,
        # but in a somewhat static workflow on a schedule we do have tasks for all partitions for every instanstiation,
        # or, also if for a run of the workflow there is actually nothing to do, prep was not even called, and we want
        # to support just calling this function anyway and nothing will happen.
        logging.info(f'Skipping partition {partition}, nothing to do...')
        run(conn, 'ROLLBACK;')
        return

    # If we are still here, len(todo_rows) equals one, and it has the ts_nodash we have to do
    ts_nodash = todo_rows[0]['ts_nodash']
    source_table = todo_rows[0]['_scd_source_table']

    ###############################
    run(conn, 'COMMIT;')
    ###############################

    # Create temp staging table from source table
    run(conn, """
    CREATE TEMPORARY TABLE IDENTIFIER(%(staging_table)s) AS
    SELECT
        {column_names:s}
    , NULL::VARCHAR AS _scd_normalised_key
    , NULL::VARCHAR AS _scd_created_by
    FROM IDENTIFIER(%(source_table)s)
    WHERE FALSE
    ;
    """.format(
        column_names='\n      , '.join([f'{column:s}' for column in columns]),
    ), params={'staging_table': staging_table, 'source_table': source_table})
    
    ###############################
    run(conn, 'BEGIN TRANSACTION;')
    ###############################
    _stage_partition_batches(conn, source_table, staging_table, columns, unique_compound_key, ts_nodash_col, ts_nodash, partitions=partitions, run_id=run_id)
    if fail_on_duplicates:
        is_key_unique = test_unique_compound_key(conn, staging_table, ('_scd_normalised_key',))
        if not is_key_unique:
            msg = f'_scd_normalised_key not unique in table {staging_table}'
            raise ValueError(msg)

    # Stage changes, with priority 1
    _stage_snapshot_changes(conn, staging_table, changes_table, scd_table, columns, unique_compound_key, ts_nodash_col, ts_nodash, partitions=partitions, run_id=run_id,
                            additional_join_columns=additional_join_columns)
    if do_zombie_check:
        _stage_zombie_changes(conn, changes_table, scd_table, columns, partitions=partitions, run_id=run_id, additional_join_columns=additional_join_columns)

    # Insert highest priority row per unique key (with lowest priority number) into the scd table clone from changes table and SCD table
    _insert_changed_and_unchanged(conn, changes_table, scd_table, scd_table_clone, columns, do_zombie_check, partitions=partitions, additional_join_columns=additional_join_columns)

    # Move over batch metadata line to clone
    # In the rare event that this transaction is still retried after a succesful commit that was not received by the client, nothing will be done,
    # because we check the done table batch_metadata_table_clone before we do anything
    params = {'todo_batch_metadata_table': todo_batch_metadata_table, 'batch_metadata_table_clone': batch_metadata_table_clone,
              'ts_nodash_col': ts_nodash_col, 'ts_nodash': ts_nodash, 'run_id': run_id}
    if partition is not None:
        params.update({f'__value_partition_0_column_{col}__': v for col, v in partition.items()})
    run(conn, """
    INSERT INTO IDENTIFIER(%(batch_metadata_table_clone)s)
    (
        {ts_nodash_col:s}  -- IDENTIFIER not supported inside INSERT INTO column specification for some reason
      {partition_columns}
      , _scd_source_table
      , _scd_created_by
      , _scd_batch_metadata
    )
    SELECT
        IDENTIFIER(%(ts_nodash_col)s)
      {partition_columns}
      , _scd_source_table
      , %(run_id)s -- Overwriting run_id because the caller of this function is the one who computed the diff for this batch partition.
                   -- Note that also if you included a _scd_created_timestamp TIMESTAMP_NTZ NOT NULL DEFAULT SYSDATE() column, it will be
                   -- set here again as well (rather than copied over from the todo_batch_metadata_table).
      , _scd_batch_metadata
    FROM IDENTIFIER(%(todo_batch_metadata_table)s) AS todo
    WHERE
          IDENTIFIER(%(ts_nodash_col)s) = %(ts_nodash)s
      {partitions_clause:s}
    ;
    """.format(
            ts_nodash_col=ts_nodash_col,
            partition_columns = ', ' + '\n      , '.join(partition_columns) if partition is not None else '',
            partitions_clause = _get_or_clauses_partitions(partitions) if partitions else '',
    ), params=params)

    ####################
    run(conn, 'COMMIT;')
    ####################

    conn.close()


def prep_batch(get_conn_callback, working_schema, table_name, scd_table, batch_metadata_table, 
               ts_nodash_col, do_zombie_check=True, partitions=None, run_id=None, batch_metadata=None, ts_nodash=None, source_table=None):
    """
    Before doing partitions for multiple tables in parallel, prepare some working tables.

    Parameters
    ----------
    get_conn_callback : object
        A function that can be called without arguments to get a Snowflake Python Connector database connection
    working_schema : str
        Fully qualified schema name for the schema that can be used to create
        working objects; 
    table_name : str
        A conceptual table name, used to name some objects in working schema uniquely
    scd_table : str
        Your SCD Type II table.
    batch_metadata_table : str
        Your batch metadata table.
    ts_nodash_col : str
        Which column in the source, staging, changes and scd tables contains the batch id timestamp (in Airflow's ts_nodash format)
    do_zombie_check : boolean, optional
        If True (default) then it is handled correctly if rows disappear from a full load, only to reappear again later.
        Computationally this is somewhat expensive, so you can disable. But if you do so and it does happen you will have multiple
        valid rows for a single normalised key as a result.
    partitions : list, optional
        Which partitions you would like to do for the current batch. If not specified, work on the
        entire input table. Format: [{col1: val1, ...}, ...]
    run_id : str, optional
        An identifier for the run that is invoking this function
    batch_metadata : dict, optional
        Anything you would like to add about the ts_nodash (the batch), will be
        serialized as JSON and stored batch_metadata_table._scd_batch_metadata VARIANT
    ts_nodash : str, optional
        The value of the batch you want to stage, in Airflow's ts_nodash format.
        Not specifying is a convenience feature: it would allow you to say:
        "this workflow instantiation I don't have anything to do".  Other task
        instances of the workflow can still be called and will work correctly,
        just also not really doing anything.  It frees you from having to
        communicate between tasks in your workflow (which is cumbersome in some
        orchestration tools, for example Airflow, where you would need to use
        XCom, which has a buggy reputation).
    source_table : str, optional
        Fully qualified name for source table for this batch. Optional only if you also do not specify ts_nodash.
    """
    if partitions:
        partition_columns = partitions[0].keys()
        for partition in partitions:
            if frozenset(partition.keys()) ^ frozenset(partition_columns):
                raise ValueError('The same partition columns should be used for each partition')

    conn = get_conn_callback()

    # Some names
    scd_table_clone = f'{working_schema}._scd_clone_{table_name}'
    todo_batch_metadata_table = f'{working_schema}._scd_todo_batch_metadata_{table_name}'
    batch_metadata_table_clone = f'{working_schema}._scd_batch_metadata_clone_{table_name}'

    # Bring over stuff to work on
    # Zero-copy clone of huge table, will take almost no time :-)
    run(conn, 'CREATE OR REPLACE TABLE IDENTIFIER(%(scd_table_clone)s) CLONE IDENTIFIER(%(scd_table)s);',
        params={'scd_table_clone': scd_table_clone, 'scd_table': scd_table})  # commits, idempotent
    run(conn, 'CREATE OR REPLACE TABLE IDENTIFIER(%(batch_metadata_table_clone)s) CLONE IDENTIFIER(%(batch_metadata_table)s);', 
        params={'batch_metadata_table_clone': batch_metadata_table_clone,
                'batch_metadata_table': batch_metadata_table})  # commits, idempotent
    run(conn, 'CREATE OR REPLACE TABLE IDENTIFIER(%(todo_batch_metadata_table)s) LIKE IDENTIFIER(%(batch_metadata_table_clone)s);', 
        params={'todo_batch_metadata_table': todo_batch_metadata_table,
                'batch_metadata_table_clone': batch_metadata_table_clone})  # commits, idempotent
    
    # Store todo partitions
    if ts_nodash is not None:
        run(conn, 'BEGIN TRANSACTION;')
        _store_batch_metadata(conn, batch_metadata_table, todo_batch_metadata_table, ts_nodash_col, ts_nodash, source_table, partitions=partitions, run_id=run_id, batch_metadata=batch_metadata)
        _delete_todo_partitions_from_clone(conn, scd_table_clone, do_zombie_check, partitions=partitions)
        run(conn, 'COMMIT;')  # idempotent transaction

    conn.close()


def deploy_batch(get_conn_callback, working_schema, table_name, scd_table, batch_metadata_table):
    """
    Finally deploying the whole batch for the table (all partitions). This will REPLACE your entire SCD Type II table.

    Parameters
    ----------
    get_conn_callback : object
        A function that can be called without arguments to get a Snowflake Python Connector database connection
    working_schema : str
        Fully qualified schema name for the schema that can be used to create
        working objects; 
    table_name : str
        A conceptual table name, used to name some objects in working schema uniquely

    Notes
    -----
    In the same breath, this function deploys the batch metadata, which can act as a log about which batches have
    been applied on your SCD Type II table.

    However, for performance reasons zero-copy cloning is used. This is a DDL statement. Therefore, while the two
    tables are cloned in the same breath, they are not cloned in the same transaction. 

    The SCD table is cloned first. In the worst case, when a connection timeout or some other error happens just after
    that one, automatic retries of your call to this function fail as well, and nobody manually retries this function
    call after fixing the connection issue, then you may end up applying the same batch again when you restart the
    whole workflow. That should not be a problem, because that should be an idempotent operation. To see why, 
    consider that the latest batch you applied will be present in the SCD table in its entirety, and it will be active:
    partially as new inserts, partially as updates of previous versions, and partially as rows that were already there
    were not changed, and remained active therefore. Then, it follows that if you apply the same batch again immediately
    after:
      - there will be no new inserts
      - there will be no updates
      - there will be no deletes: if any rows were missing the first time the batch was applied, they would already have 
        been marked as deleted
      - there will be no invalidations of already deleted rows, because there are no new inserts
    """
    # Some names
    scd_table_clone = f'{working_schema}._scd_clone_{table_name}'
    batch_metadata_table_clone = f'{working_schema}._scd_batch_metadata_clone_{table_name}'
    todo_batch_metadata_table = f'{working_schema}._scd_todo_batch_metadata_{table_name}'

    with get_conn_callback() as conn:

        run(conn, 'BEGIN TRANSACTION')
        row_count_todo = run_and_fetchall(conn, 'SELECT COUNT(*) AS n_partitions FROM IDENTIFIER(%(todo_batch_metadata_table)s);',
                                        params={'todo_batch_metadata_table': todo_batch_metadata_table})[0]['n_partitions']
        # Although it costs nothing almost, it feelts off to deploy the clone if nothing was done
        if row_count_todo == 0:
            logging.info('Skipping deploy... nothing was done')
            conn.commit()
            return

        # Some minimal validation to have better sleep; callers of this function can always do more themselves
        row_count_scd_table_clone = run_and_fetchall(conn, 'SELECT COUNT(*) AS n_scd_table_clone FROM IDENTIFIER(%(scd_table_clone)s);',
                                                     params={'scd_table_clone': scd_table_clone})[0]['n_scd_table_clone']
        row_count_scd_table = run_and_fetchall(conn, 'SELECT COUNT(*) AS n_scd_table FROM IDENTIFIER(%(scd_table)s);',
                                               params={'scd_table': scd_table})[0]['n_scd_table']
        row_count_batch_metadata_table_clone = run_and_fetchall(conn, 'SELECT COUNT(*) AS n_partitions_clone FROM IDENTIFIER(%(batch_metadata_table_clone)s);',
                                                     params={'batch_metadata_table_clone': batch_metadata_table_clone})[0]['n_partitions_clone']
        row_count_batch_metadata_table = run_and_fetchall(conn, 'SELECT COUNT(*) AS n_partitions FROM IDENTIFIER(%(batch_metadata_table)s);',
                                                     params={'batch_metadata_table': batch_metadata_table})[0]['n_partitions']
        conn.commit()

        if row_count_scd_table_clone < row_count_scd_table:
            raise ValueError('Fewer rows in SCD table clone')
        if row_count_batch_metadata_table_clone - row_count_batch_metadata_table > row_count_todo:  
            raise ValueError('Too many rows in batch metadata table clone')
        if row_count_batch_metadata_table_clone == row_count_batch_metadata_table:
            logging.warn('This should only very rarely happen, in the case a succesfully committed transaction was still retried')
        else:
            if row_count_batch_metadata_table_clone - row_count_batch_metadata_table < row_count_todo:  
                raise ValueError('Too few rows in batch metadata table clone')

        # COPY GRANTS will copy grants from to be replaced table.
        # Both statements will commit. If the second fails, a partial write is the result.
        # Deploy, SCD table first ;-)
        run(conn, 'CREATE OR REPLACE TABLE IDENTIFIER(%(scd_table)s) CLONE IDENTIFIER(%(scd_table_clone)s) COPY GRANTS',
            params={'scd_table': scd_table, 'scd_table_clone': scd_table_clone})
        run(conn, 'CREATE OR REPLACE TABLE IDENTIFIER(%(batch_metadata_table)s) CLONE IDENTIFIER(%(batch_metadata_table_clone)s) COPY GRANTS',
            params={'batch_metadata_table': batch_metadata_table, 'batch_metadata_table_clone': batch_metadata_table_clone})
        

def clean_up_batch(get_conn_callback, working_schema, table_name):
    """
    Drop bunch of tables (if they exist, to make this function idempotent in case of retries)

    Parameters
    ----------
    get_conn_callback : object
        A function that can be called without arguments to get a Snowflake Python Connector database connection
    working_schema : str
        Fully qualified schema name for the schema that can be used to create
        working objects; 
    table_name : str
        A conceptual table name, used to name some objects in working schema uniquely
    """
    conn = get_conn_callback()

    # Some names
    scd_table_clone = f'{working_schema}._scd_clone_{table_name}'
    todo_batch_metadata_table = f'{working_schema}._scd_todo_batch_metadata_{table_name}'
    batch_metadata_table_clone = f'{working_schema}._scd_batch_metadata_clone_{table_name}'

    # The working schema has to be non-transient or cloning will not work. Therefore, after cloning has succeeded, but before dropping the tables,
    # manually set data retention time in days to zero
    run(conn, 'ALTER TABLE IF EXISTS IDENTIFIER(%(todo_batch_metadata_table)s) SET DATA_RETENTION_TIME_IN_DAYS = 0;', params={'todo_batch_metadata_table': todo_batch_metadata_table})
    run(conn, 'ALTER TABLE IF EXISTS IDENTIFIER(%(batch_metadata_table_clone)s) SET DATA_RETENTION_TIME_IN_DAYS = 0;', params={'batch_metadata_table_clone': batch_metadata_table_clone})
    run(conn, 'ALTER TABLE IF EXISTS IDENTIFIER(%(scd_table_clone)s) SET DATA_RETENTION_TIME_IN_DAYS = 0;', params={'scd_table_clone': scd_table_clone})

    run(conn, 'DROP TABLE IF EXISTS IDENTIFIER(%(todo_batch_metadata_table)s);', params={'todo_batch_metadata_table': todo_batch_metadata_table})
    run(conn, 'DROP TABLE IF EXISTS IDENTIFIER(%(batch_metadata_table_clone)s);', params={'batch_metadata_table_clone': batch_metadata_table_clone})
    run(conn, 'DROP TABLE IF EXISTS IDENTIFIER(%(scd_table_clone)s);', params={'scd_table_clone': scd_table_clone})

    conn.close()


def snapshot(get_conn_callback, working_schema, table_name, source_table, scd_table, batch_metadata_table, columns, unique_compound_key, ts_nodash_col, ts_nodash,
             partitions=None, fail_on_duplicates=True, run_id=None, batch_metadata=None, do_zombie_check=True, additional_join_columns=None):
    """
    Convenience function for if you want to use serialized execution after all

    Parameters
    ----------
    get_conn_callback : object
        A function that can be called without arguments to get a Snowflake Python Connector database connection
    working_schema : str
        Fully qualified schema name for the schema that can be used to create
        working objects; 
    table_name : str
        A conceptual table name, used to name some objects in working schema uniquely
    source_table : str
        Table you want to stage data from. Should have a column with a batch timestamp in the Airflow ts_nodash format.
    scd_table : str
        Your SCD Type II table.
    batch_metadata_table : str
        Your batch metadata table.
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
    ts_nodash : str, optional
        The value of the batch you want to stage, in Airflow's ts_nodash format.
    partitions : list, optional
        Which partitions you would like to do for the current batch. If not specified, work on the
        entire input table. Format: [{col1: val1, ...}, ...]
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
    additional_join_columns: iterable, optional
        In the process of snapshotting, or, building a SCD Type II table, we it
        is customary to compute surrogate keys by hashing unique compound key
        columns. These surrogate keys are then used in equijoins. While
        convenient, this does take away information from the query optimizer.
        Moreover, it looses sort order of the original columns.  Snowflake
        stores its data in micropartitions.  For each micropartitions, metadata
        is maintained, such as the minimal and maximal values for each column.
        The range of values in each of these micropartitions for a value like an
        MD5 hash can be expected to overlap a great deal in each of the
        micropartitions.  This means that the query optimizer can not expect to
        determine based on metadata if two micropartitions it is considering to
        join can or cannot produce rows. In other words, it cannot use pruning
        (min-max pruning).  An alternative might be to always join on all the
        unique compound key columns, rather than on a surrogate key.
        Experimentation would be required to determine if that could indeed
        speed up the process. For now, in this module, you have the option to
        use no additional columns in equijoins, only a few (start with low
        cardinality ones that split your data in a few big chunks), or all of
        the unique key columns.  If you add all of them, in effect, this module
        is doing one superfluous comparison of a surrogate key. Finally, if you
        are calling this function with partitions, then it would likely be of no use to
        include partition columns in the additional_join_columns, as from both sides of
        the join rows from different partitions are already filtered out (one would hope
        the query optimizer uses that information already to do pruning). 
    
    Notes
    -----
    Call this function if you just want sequential treatments of partitions
    """
    if frozenset(unique_compound_key) - frozenset(columns):
        raise ValueError('unique_compound_key Should be a subset of columns')
    if additional_join_columns is not None and frozenset(additional_join_columns) - frozenset(unique_compound_key):
        raise ValueError('additional_join_columns Should be a subset of unique_compound key')
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
            
    # Prepare the entire batch
    prep_batch(get_conn_callback, working_schema, table_name, scd_table, batch_metadata_table, ts_nodash_col,
               do_zombie_check=do_zombie_check, partitions=partitions, run_id=run_id, batch_metadata=batch_metadata, ts_nodash=ts_nodash, source_table=source_table)

    if not partitions: 
        # do the whole source table at once
        snapshot_partition(get_conn_callback, working_schema, table_name, scd_table, columns, unique_compound_key, ts_nodash_col,
                           partition=None, fail_on_duplicates=fail_on_duplicates, run_id=run_id, do_zombie_check=do_zombie_check, additional_join_columns=additional_join_columns)
    else:
        for partition in partitions:
            snapshot_partition(get_conn_callback, working_schema, table_name, scd_table, columns, unique_compound_key, ts_nodash_col,
                               partition=partition, fail_on_duplicates=fail_on_duplicates, run_id=run_id, do_zombie_check=do_zombie_check, additional_join_columns=additional_join_columns)

    # TODO: offer a validation function over multiple tables, to check that the same batches are in the done table
    # Callers themselves could implement logic like checking referential integrity for known foreign keys. (or this module could be
    # extended with that functionality, alternatively)

    # Deploy the entire batch
    deploy_batch(get_conn_callback, working_schema, table_name, scd_table, batch_metadata_table)


    # Clean up batch
    clean_up_batch(get_conn_callback, working_schema, table_name)


def create_scd_table(get_conn_callback, column_types, ts_no_dash_col, scd_table, batch_metadata_table, partition_columns=None):
    """
    Create the SCD Type II table and batch metadata table if they do not yet exist.

    Parameters
    ----------
    get_conn_callback : object
    column_types : iterable
        An iterable with iterables in it that have two elements: column name, and sth like 'VARCHAR NOT NULL' (i.e., type and constraints in one string)
    scd_table : str
        Fully qualified desired name of table to create.
    batch_metadata_table : str
        Fully qualified desired name of batch metadata table to create

    Notes
    -----
    Supports no schema evolution as of yet.

    If it would one day, then, most likely declarative. So a rename column is then out
    of the question :-)

    You always have the option to use your favourite schema management tool, e.g., Sqitch or whatever, to create the tables instead.
    In that case, however, you would have to transfer ownership of the table to whatever role you assume when you call this function:
    to do a CREATE OR REPLACE on a table that is the minimum requirement in Snowflake.

    TODO: issue a warning if ts_nodash_col its type is not VARCHAR NOT NULL, or something like that.
    """
    conn = get_conn_callback()

    run(
            conn,
            """
            CREATE TABLE IF NOT EXISTS IDENTIFIER(%(scd_table)s)
            (
                {sql_column_types:s}
              , _scd_normalised_key VARCHAR NOT NULL
              , _scd_id VARCHAR NOT NULL
              , _scd_valid_from_timestamp TIMESTAMP_NTZ NOT NULL
              , _scd_valid_to_timestamp TIMESTAMP_NTZ
              , _scd_deleted_timestamp TIMESTAMP_NTZ
              , _scd_is_most_recent BOOLEAN NOT NULL
              , _scd_last_modified_timestamp TIMESTAMP_NTZ NOT NULL
              , _scd_last_modified_by VARCHAR
              , _scd_last_dml_action VARCHAR NOT NULL
              , _scd_created_timestamp TIMESTAMP_NTZ NOT NULL
              , _scd_created_by VARCHAR
            )""".format(
                    sql_column_types='\n              , '.join([f'{col} {type_}' for col, type_ in column_types])
            ),
            params={'scd_table': scd_table}
    )

    run(
            conn,
            """
            CREATE TABLE IF NOT EXISTS IDENTIFIER(%(batch_metadata_table)s)
            (
                {ts_no_dash_col} VARCHAR NOT NULL
              {sql_partition_column_types:s}
              , _scd_source_table VARCHAR
              , _scd_batch_metadata VARIANT
              , _scd_created_timestamp TIMESTAMP_NTZ NOT NULL DEFAULT SYSDATE()
              , _scd_created_by VARCHAR
            )""".format(
                    ts_no_dash_col=ts_no_dash_col,
                    sql_partition_column_types='' if partition_columns is None else '              , ' + '\n              , '.join(
                            [f'{col} {type_}' for col, type_ in column_types if col in partition_columns]
                    )
            ),
            params={'batch_metadata_table': batch_metadata_table}
    )

    conn.close()