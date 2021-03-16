package song.pan.etl.rdbms.impl;

import lombok.extern.slf4j.Slf4j;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import song.pan.etl.common.exception.IllegalArgumentException;
import song.pan.etl.common.exception.InvalidSQLException;
import song.pan.etl.common.exception.SystemException;
import song.pan.etl.rdbms.AbstractRdbmsServer;
import song.pan.etl.rdbms.DataType;
import song.pan.etl.rdbms.RdbmsType;
import song.pan.etl.rdbms.element.*;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Types;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.regex.Pattern;
import java.util.stream.Collectors;


import static song.pan.etl.rdbms.DataType.of;

/**
 * @author Song Pan
 * @version 1.0.0
 */
@Slf4j
public class SybaseServer extends AbstractRdbmsServer {


    protected static final List<DataType> dataTypes = new LinkedList<>();


    static {
        // bit
        dataTypes.add(of(Types.BIT, "BIT", boolean.class));

        // blob
        dataTypes.add(of(Types.BLOB, "BLOB", byte[].class));

        // string
        dataTypes.add(of(Types.CHAR, "CHAR", String.class));
        dataTypes.add(of(Types.VARCHAR, "VARCHAR", String.class));
        dataTypes.add(of(Types.VARCHAR, "TEXT", String.class));
        dataTypes.add(of(Types.CLOB, "CLOB", String.class));

        // Number
        dataTypes.add(of(Types.TINYINT, "TINYINT", Byte.class));
        dataTypes.add(of(Types.SMALLINT, "SMALLINT", int.class));
        dataTypes.add(of(Types.INTEGER, "INT", int.class));
        dataTypes.add(of(Types.BIGINT, "BIGINT", long.class));
        dataTypes.add(of(Types.DECIMAL, "DECIMAL", BigDecimal.class));
        dataTypes.add(of(Types.FLOAT, "FLOAT", Double.class));
        dataTypes.add(of(Types.DOUBLE, "DOUBLE", Double.class));
        dataTypes.add(of(Types.NUMERIC, "NUMERIC", BigDecimal.class));

        dataTypes.add(of(Types.ROWID, "NUMERIC IDENTITY", long.class));
        dataTypes.add(of(Types.ROWID, "IDENTITY", long.class));

        // Date
        dataTypes.add(of(Types.DATE, "DATE", java.sql.Date.class));
        dataTypes.add(of(Types.TIMESTAMP, "DATETIME", java.sql.Timestamp.class));
        dataTypes.add(of(Types.TIMESTAMP, "SMALLDATETIME", java.sql.Timestamp.class));
        dataTypes.add(of(Types.TIME, "TIME", java.sql.Time.class));
    }


    @Override
    protected List<DataType> getAllDataTypes() {
        return dataTypes;
    }

    @Override
    public RdbmsType getType() {
        return RdbmsType.SYBASE;
    }


    /**
     * Table name pattern:
     * - name
     * - catalog..name
     * - catalog.schema.name
     */
    private static final Pattern NOT_CONTAINS_DOT = Pattern.compile("((?!\\.).)*");
    private static final Pattern CATALOG_NAME = Pattern.compile("((?!\\.).)*\\.\\.((?!\\.).)*");
    private static final Pattern CATALOG_SCHEMA_NAME = Pattern.compile("((?!\\.).)*\\.((?!\\.).)*\\.((?!\\.).)*");


    @Override
    public Table fromFullQualifiedName(String name) {
        if (null == name) {
            return null;
        }

        if (!NOT_CONTAINS_DOT.matcher(name).matches()) {
            return new Table(name);
        }

        if (!CATALOG_NAME.matcher(name).matches()) {
            String[] s = name.split("\\.\\.");
            return new Table(s[0], s[1]);
        }

        if (!CATALOG_SCHEMA_NAME.matcher(name).matches()) {
            String[] s = name.split("\\.");
            return new Table(s[0], s[1], s[2]);
        }

        throw new IllegalArgumentException("Unknown table name format: " + name);
    }

    @Override
    public String fullQualifiedNameOf(Table table) {
        if (null != table.getCatalog()) {
            if (null != table.getSchema()) {
                return String.join(".", table.getCatalog(), table.getSchema(), table.getName());
            } else {
                return String.join("..", table.getCatalog(), table.getName());
            }
        }
        return table.getName();
    }

    @Override
    public List<String> getCatalogs() {
        return getJdbcTemplate().queryForList("sp_helpdb").stream()
                .map(e -> (String) e.get("name"))
                .collect(Collectors.toList());
    }

    @Override
    public boolean isTableExist(Table table) {
        String query = "SELECT COUNT(1) FROM " + table.getCatalog() + "..sysobjects o WHERE o.name = '" +
                table.getName() + "' AND o.type = 'U'";
        Integer cnt = getJdbcTemplate().queryForObject(query, Integer.class);
        if (cnt == null) {
            throw new SystemException("Check table existence return null: " + query);
        }
        return cnt > 0;
    }

    @Override
    public void renameTable(Table old, String newName) {
        JdbcTemplate jdbcTemplate = getJdbcTemplate();
        jdbcTemplate.update("USE " + old.getCatalog());
        jdbcTemplate.update("sp_rename " + old.getName() + ", " + newName);
    }

    @Override
    public void validate(String query) {
        try {
            getJdbcTemplate().queryForObject("SELECT TOP 1 1 col FROM (" + query + ") t", Integer.class);
        } catch (EmptyResultDataAccessException e) {
            // empty table
        } catch (Exception e) {
            throw new InvalidSQLException(e);
        }
    }

    @Override
    public void copyToGenerateId(String query, Table to, String uid) {
        JdbcTemplate jdbcTemplate = getJdbcTemplate();

        String toTable = fullQualifiedNameOf(to);
        if (isTableExist(to)) {
            log.info("[Sybase#copyToGenerateId] table {} exists, drop it", toTable);
            jdbcTemplate.update("DROP TABLE " + toTable);
        }
        String stmt = "CREATE TABLE " + toTable + "(" + uid + " numeric identity," +
                to.getColumns().stream().map(this::format).collect(Collectors.joining(",")) + ")";
        log.info("[Sybase#copyToGenerateId] {}", stmt);
        jdbcTemplate.update(stmt);

        String joinedNames = to.getColumns().stream().map(Column::getName).collect(Collectors.joining(","));
        stmt = "INSERT INTO " + toTable + "(" + joinedNames + ") SELECT " + joinedNames + " FROM (" + query + ") t";
        log.info("[Sybase#copyToGenerateId] {}", stmt);
        jdbcTemplate.update(stmt);
    }

    @Override
    public String format(Object value) {
        if (value instanceof Date) {
            Date date = (Date) value;
            return "Convert(datetime, '" + new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(date) + "', 121)";
        } else if (value instanceof CharSequence) {
            return "'" + value + "'";
        }

        return String.valueOf(value);
    }

    @Override
    public String format(Column column) {
        String defaultValue = "";
        if (null != column.getDefaultValue()) {
            defaultValue = "default " + column.getDefaultValue();
        }

        String nullable = "";
        if (Boolean.TRUE.equals(column.getNullable())) {
            nullable = "null";
        }

        String type = column.getType();
        if (type.toLowerCase().contains("char") && column.getLength() > 0) {
            type = type + "(" + column.getLength() + ")";
        }

        return String.join(" ", column.getName(), type, defaultValue, nullable);
    }

    @Override
    public List<Row> topRowsOf(String query, int num) {
        int fromIndex = query.toLowerCase().indexOf("select ") + 7;
        return getJdbcTemplate().queryForList("SELECT TOP " + num + " " + query.substring(fromIndex))
                .stream().map(Row::fromMap).collect(Collectors.toList());
    }

    @Override
    public List<Column> columnsOf(Table table) {
        JdbcTemplate jdbcTemplate = getJdbcTemplate();
        jdbcTemplate.update("USE " + table.getCatalog());
        return jdbcTemplate.queryForList("sp_columns " + table.getName())
                .stream()
                .map(e -> new Column((String) e.get("column_name"), (String) e.get("type_name"), null, (int) e.get("length")))
                .peek(this::setTypeIndex)
                .collect(Collectors.toList());
    }


    @Override
    public List<String> primaryKeyOf(Table table) {
        // find identity column
        for (Column column : columnsOf(table)) {
            if (column.getType().toLowerCase().contains("identity")) {
                return Collections.singletonList(column.getName());
            }
        }

        // find primary key
        String query = "SELECT indid, keycnt FROM " + table.getCatalog() +
                "..sysindexes WHERE status&2048=2048 AND id = object_id('" + table.getName() + "')";
        List<Map<String, Object>> sysindexes = getJdbcTemplate().queryForList(query);
        if (sysindexes.isEmpty()) {
            return Collections.emptyList();
        }
        Map<String, Object> sysindex = sysindexes.get(0);

        // the value of indid and keycnt could be a Short type value, do not cast to int directly
        Object indid0 = sysindex.get("indid");
        int indid = indid0 instanceof Integer ? (int) indid0 : Integer.parseInt(indid0.toString());
        Object keycnt0 = sysindex.get("keycnt");
        int keycnt = keycnt0 instanceof Integer ? (int) keycnt0 : Integer.parseInt(keycnt0.toString());

        return columnNamesOf(table, indid, keycnt);
    }


    private List<String> columnNamesOf(Table table, int indid, int keycnt) {
        StringBuilder indexName = new StringBuilder("SELECT ");
        for (int i = 0; i < keycnt; i++) {
            if (i > 0) {
                indexName.append(",");
            }
            indexName.append("index_col('").append(table.getName()).append("',").append(indid).append(",").append(i).append(")")
                    .append(" idx_").append(i);
        }
        indexName.append(" FROM ").append(table.getCatalog()).append("..sysindexes WHERE indid = ").append(indid)
                .append(" and id = object_id('").append(table.getName()).append("')");
        List<Map<String, Object>> indexNames = getJdbcTemplate().queryForList(indexName.toString());
        if (indexNames.isEmpty()) {
            return Collections.emptyList();
        }
        Map<String, Object> indexNameMap = indexNames.get(0);
        List<String> columns = new LinkedList<>();
        for (int i = 0; i < keycnt; i++) {
            Optional.ofNullable((String) indexNameMap.get("idx_" + i)).ifPresent(columns::add);
        }
        return columns;
    }


    @Override
    public List<Index> indexesOf(Table table) {
        String query = "SELECT name, indid, keycnt FROM " + table.getCatalog() +
                "..sysindexes WHERE status&2048!=2048 AND indid > 0 and indid < 255 AND id = object_id('" + table.getName() + "')";

        List<Map<String, Object>> sysindexes = getJdbcTemplate().queryForList(query);
        if (sysindexes.isEmpty()) {
            return Collections.emptyList();
        }

        return sysindexes.stream().map(sysindex -> {
            Object indid0 = sysindex.get("indid");
            int indid = indid0 instanceof Integer ? (int) indid0 : Integer.parseInt(indid0.toString());
            Object keycnt0 = sysindex.get("keycnt");
            int keycnt = keycnt0 instanceof Integer ? (int) keycnt0 : Integer.parseInt(keycnt0.toString());

            List<String> columnNames = columnNamesOf(table, indid, keycnt);
            SybaseIndex index = new SybaseIndex();
            index.setName((String) sysindex.get("name"));
            index.setColumns(columnNames);
            index.setUnique(unique(table, indid));
            index.setClustered(clustered(table, indid));
            return index;
        }).collect(Collectors.toList());
    }


    private boolean unique(Table table, int indid) {
        String query = "SELECT v.name FROM master.dbo.spt_values v, " + table.getCatalog() + "..sysindexes i " +
                "WHERE i.status & v.number = v.number " +
                "AND v.type = 'I' and v.number = 2 " +
                "AND i.id = object_id('" + table.getName() + "') " +
                "AND i.indid = " + indid;
        List<Map<String, Object>> unique = getJdbcTemplate().queryForList(query);
        if (unique.isEmpty()) {
            return false;
        }
        return "unique".equals(unique.get(0).get("name"));
    }


    private boolean clustered(Table table, int indid) {
        String query = "SELECT COUNT(1) FROM " + table.getCatalog() + "..sysindexes i " +
                "WHERE status2 & 512 = 512 " +
                "AND i.id = object_id('" + table.getName() + "') " +
                "AND i.indid = " + indid;
        Integer cnt = getJdbcTemplate().queryForObject(query, Integer.class);
        return cnt != null && cnt > 0;
    }


}
