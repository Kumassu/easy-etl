package song.pan.etl.rdbms;


import song.pan.etl.common.exception.InvalidSQLException;
import song.pan.etl.rdbms.element.*;

import java.util.List;

/**
 * Represent a common RDBMS server,
 * hides the differences between databases,
 * provides common API for ETL process.
 *
 *
 * @author Song Pan
 * @version 1.0
 */
public interface RdbmsServer extends AlivenessDetectable, Lifecycle, EasyConnect {


    /**
     * @return a type to identify current server
     */
    RdbmsType getType();


    /**
     * Parse a table's full qualified name to {@link Table}
     *
     * @param name full qualified name, the format may vary among database implementations, e.g.
     *             - sybase: catalog..table, catalog.schema.table
     *             - sqlserver: [catalog]..[table], [catalog].[schema].[table]
     *             - mysql: catalog.table
     * @return an instance of {@link Table}
     */
    Table fromFullQualifiedName(String name);


    /**
     * Parse a {@link Table} to its full qualified name
     * @param table target table
     * @return full qualified name
     */
    String fullQualifiedNameOf(Table table);


    /**
     * @return all the catalogs (may also called database) of current server
     */
    List<String> getCatalogs();


    boolean isTableExist(Table table);


    void renameTable(Table old, String newName);


    void createTable(Table table);


    void truncateTable(Table table);


    /**
     * Copy a table's data to other table
     * @param from data source
     * @param to destination
     */
    void copyTable(Table from, Table to);


    /**
     * Copy query results to a new table to generate a auto-increased column
     *
     * @param query source data
     * @param to destination table
     * @param uid the auto increased id column
     */
    void copyToGenerateId(String query, Table to, String uid);


    void dropTable(Table table);


    long count(String query);


    /**
     * Execute a DML/DDL script in default catalog
     * @param sql script to execute
     */
    void execute(String sql);


    /**
     * Execute a DML/DDL script in the specified catalog
     * @param sql script to execute
     */
    void execute(String catalog, String sql);


    /**
     * Validate query statement
     * @param query query to validate
     * @throws InvalidSQLException if invalid
     */
    void validate(String query);


    /**
     * Transform object to SQL format, e.g.
     * String -> 'value'
     * Date -> '2021-03-12'
     * @param value object to format
     * @return sql string format
     */
    String format(Object value);



    /**
     * Transform {@link Column} to SQL format,
     * used when create table or alter table to add column, e.g.
     * {
     *     "name" : "id",
     *     "type" : "identity",
     *     "typeIndex" : 1
     * }
     * will be transferred to
     * [id] [int] identity(1,1) in sqlserver.
     * @param column column to format
     * @return sql string format
     */
    String format(Column column);


    /**
     * @return Return the number of top rows in the query results
     */
    List<Row> topRowsOf(String query, int num);


    List<Column> columnsOf(Table table);


    List<Column> columnsOf(String query);


    List<String> primaryKeyOf(Table table);


    List<Index> indexesOf(Table table);


    /**
     * Find the max value of a column in the query results
     */
    Object maxOf(String query, String column);


    /**
     * Find the min value of a column in the query results
     */
    Object minOf(String query, String column);


    /**
     * Order the query results by specified columns,
     * return the data of the last row
     * @param query result set
     * @param orderBys column names to order by
     * @return the data of the last row
     */
    Row lastRowOf(String query, List<String> orderBys);


    /**
     * Query one page of data between the start and end boundary
     * @param column pagination key to split results
     * @param startBoundary where pagination start from
     * @param endBoundary where pagination end at
     * @return a page of data
     */
    List<Row> query(String query, Column column, Object startBoundary, Object endBoundary);


    /**
     * Query one page of data via offset and limit
     * @param offset page start index
     * @param limit page size
     * @return a page of data
     */
    List<Row> query(String query, long offset, long limit);


    /**
     * @return the query results
     */
    List<Row> query(String query);


    /**
     * Prepare insert statement using question mark as placeholder
     * e.g.
     * INSERT INTO TABLE_N(COLUMN_N,COLUMN_M) VALUES (?,?)
     * @param table target table where data insert into
     * @return prepared insert statement
     */
    String prepareInsertStatement(Table table);


    /**
     * Prepare insert statement using column name as placeholder
     * e.g.
     * INSERT INTO TABLE_N(COLUMN_N,COLUMN_M) VALUES (:COLUMN_N,:COLUMN_M)
     * @param table target table where data insert into
     * @return prepared insert statement
     */
    String prepareNamedInsertStatement(Table table);


    /**
     * Generate a query statement selects
     * all the columns of the specified table
     * @param table target table to select
     * @return query statement to select whole table
     */
    String select(Table table);


    /**
     * Find {@link DataType} by type name
     */
    DataType typeOf(String typeName);


    /**
     * Find {@link DataType} by type index
     */
    DataType typeOf(int typeIndex);

}
