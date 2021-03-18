package song.pan.etl.rdbms.impl;

import lombok.extern.slf4j.Slf4j;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import song.pan.etl.common.exception.IllegalArgumentException;
import song.pan.etl.common.exception.InvalidSQLException;
import song.pan.etl.common.exception.SystemException;
import song.pan.etl.rdbms.*;
import song.pan.etl.rdbms.element.*;

import java.math.BigDecimal;
import java.math.BigInteger;
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
public class MySQLServer extends AbstractRdbmsServer {

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

        // Number
        dataTypes.add(of(Types.TINYINT, "TINYINT", int.class));
        dataTypes.add(of(Types.SMALLINT, "SMALLINT", int.class));

        dataTypes.add(of(Types.INTEGER, "INT", long.class));
        dataTypes.add(of(Types.INTEGER, "INTEGER", long.class));
        dataTypes.add(of(Types.INTEGER, "MEDIUMINT", int.class));


        dataTypes.add(of(Types.BIGINT, "BIGINT", BigInteger.class));
        dataTypes.add(of(Types.DECIMAL, "DECIMAL", BigDecimal.class));
        dataTypes.add(of(Types.FLOAT, "FLOAT", Double.class));
        dataTypes.add(of(Types.DOUBLE, "DOUBLE", Double.class));
        dataTypes.add(of(Types.ROWID, "ID", long.class));

        // Date
        dataTypes.add(of(Types.DATE, "DATE", java.sql.Date.class));
        dataTypes.add(of(Types.TIMESTAMP, "DATETIME", java.sql.Timestamp.class));
        dataTypes.add(of(Types.TIMESTAMP, "TIMESTAMP", java.sql.Timestamp.class));
        dataTypes.add(of(Types.TIME, "TIME", java.sql.Time.class));
        dataTypes.add(of(Types.DATE, "YEAR", java.sql.Date.class));
    }


    @Override
    protected List<DataType> getAllDataTypes() {
        return dataTypes;
    }

    @Override
    public RdbmsType getType() {
        return RdbmsType.MYSQL;
    }

    /**
     * Table name pattern:
     * - name
     * - catalog.name
     */
    private static final Pattern NOT_CONTAINS_DOT = Pattern.compile("((?!\\.).)*");
    private static final Pattern CATALOG_NAME = Pattern.compile("((?!\\.).)*\\.((?!\\.).)*");


    @Override
    public Table fromFullQualifiedName(String name) {
        if (null == name) {
            return null;
        }

        if (!NOT_CONTAINS_DOT.matcher(name).matches()) {
            return new Table(name);
        }

        if (!CATALOG_NAME.matcher(name).matches()) {
            String[] s = name.split("\\.");
            return new Table(s[0], s[1]);
        }

        throw new IllegalArgumentException("Unknown table name format: " + name);
    }

    @Override
    public String fullQualifiedNameOf(Table table) {
        if (null != table.getCatalog()) {
            return String.join(".", table.getCatalog(), table.getName());
        }
        return table.getName();
    }

    @Override
    public List<String> catalogs() {
        return getJdbcTemplate().queryForList("show databases").stream()
                .map(e -> (String) e.get("Database")).collect(Collectors.toList());
    }

    @Override
    public List<String> tablesOf(String catalog) {
        return getJdbcTemplate().queryForList("SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES t WHERE t.TABLE_SCHEMA ='"
                + catalog + "'").stream().map(e -> (String) e.get("TABLE_NAME")).collect(Collectors.toList());
    }

    @Override
    public Table tableOf(String catalog, String name) {
        Table table = new Table(catalog, name);
        table.setColumns(columnsOf(table));
        return table;
    }

    @Override
    public boolean isTableExist(Table table) {
        String query = "select count(*) from information_schema.TABLES t where t.TABLE_SCHEMA ='" + table.getCatalog() +
                "' and t.TABLE_NAME ='" + table.getName() + "'";
        Integer cnt = getJdbcTemplate().queryForObject(query, Integer.class);
        if (cnt == null) {
            throw new SystemException("Check table existence return null: " + query);
        }
        return cnt > 0;
    }

    @Override
    public void renameTable(Table old, String newName) {
        getJdbcTemplate().update("rename table " + old.getCatalog() + "." + old.getName() +
                " to " + old.getCatalog() + "." + newName);
    }

    @Override
    public void copyToGenerateId(String query, Table to, String uid) {
        JdbcTemplate jdbcTemplate = getJdbcTemplate();

        String toTable = fullQualifiedNameOf(to);
        if (isTableExist(to)) {
            log.info("[Mysql#copyToGenerateId] table {} exists, drop it", toTable);
            jdbcTemplate.update("DROP TABLE " + toTable);
        }
        String stmt = "CREATE TABLE " + toTable + "(" + uid + " INT UNSIGNED AUTO_INCREMENT," +
                to.getColumns().stream().map(this::format).collect(Collectors.joining(",")) +
                ", PRIMARY KEY (" + uid + ")" + ")";
        log.info("[Mysql#copyToGenerateId] {}", stmt);
        jdbcTemplate.update(stmt);

        String joinedNames = to.getColumns().stream().map(Column::getName).collect(Collectors.joining(","));
        stmt = "INSERT INTO " + toTable + "(" + joinedNames + ") SELECT " + joinedNames + " FROM (" + query + ") t";
        log.info("[Mysql#copyToGenerateId] {}", stmt);
        jdbcTemplate.update(stmt);
    }

    @Override
    public void validate(String query) {
        try {
            getJdbcTemplate().queryForObject("SELECT 1 FROM (" + query + ") t limit 1", Integer.class);
        } catch (EmptyResultDataAccessException e) {
            // empty table
        } catch (Exception e) {
            throw new InvalidSQLException(e);
        }
    }

    @Override
    public String format(Object value) {
        if (value instanceof Date) {
            Date date = (Date) value;
            return "'" + new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(date) + "'";
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
        if (Boolean.FALSE.equals(column.getNullable())) {
            nullable = "not null";
        }

        String type = column.getType();
        if (column.getPrecision() > 0) {
            if (column.getScale() > 0) {
                type = type + "(" + column.getPrecision() + "," + column.getScale() + ")";
            } else {
                type = type + "(" + column.getPrecision() + ")";
            }
        } else if (column.getLength() > 0) {
            type = type + "(" + column.getLength() + ")";
        }

        return String.join(" ", column.getName(), type, defaultValue, nullable);
    }

    @Override
    public List<Row> topRowsOf(String query, long num) {
        return getJdbcTemplate().queryForList("SELECT * FROM (" + query + ") t LIMIT " + num)
                .stream().map(Row::fromMap).collect(Collectors.toList());
    }

    @Override
    public List<Column> columnsOf(Table table) {
        return getJdbcTemplate().queryForList("SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '" + table.getName() +
                "' AND TABLE_SCHEMA = '" + table.getCatalog() + "'")
                .stream()
                .map(e -> {
                    Column column = new Column((String) e.get("COLUMN_NAME"), (String) e.get("DATA_TYPE"));

                    String nullable = (String) e.get("IS_NULLABLE");
                    column.setNullable(nullable != null && nullable.equalsIgnoreCase("YES"));

                    Object numericPrecision = e.get("NUMERIC_PRECISION");
                    if (null != numericPrecision) {
                        column.setPrecision(Integer.parseInt(numericPrecision.toString()));
                    }
                    Object numericScale = e.get("NUMERIC_SCALE");
                    if (null != numericScale) {
                        column.setScale(Integer.parseInt(numericScale.toString()));
                    }
                    Object characterMaximumLength = e.get("CHARACTER_MAXIMUM_LENGTH");
                    if (null != characterMaximumLength) {
                        column.setLength(Integer.parseInt(characterMaximumLength.toString()));
                    }
                    return column;
                })
                .peek(this::setTypeIndex)
                .collect(Collectors.toList());
    }

    @Override
    public List<String> primaryKeyOf(Table table) {
        return getJdbcTemplate().queryForList("SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE WHERE " +
                "table_name='" + table.getName() +"' AND CONSTRAINT_SCHEMA='" + table.getCatalog() + "' AND constraint_name='PRIMARY'")
                .stream().map(e -> (String) e.get("COLUMN_NAME")).collect(Collectors.toList());
    }


    @Override
    public List<Index> indexesOf(Table table) {
        List<Index> indices = new LinkedList<>();
        getJdbcTemplate().queryForList("show index from " + fullQualifiedNameOf(table)).forEach(row -> {
            String keyName = (String) row.get("Key_name");
            String columnName = (String) row.get("Column_name");
            boolean unique = ((int) row.get("Non_unique")) == 0;

            Optional<Index> index = indices.stream().filter(i -> i.getName().equals(keyName)).findAny();
            if (index.isPresent()) {
                index.get().getColumns().add(columnName);
            } else {
                MySQLIndex idx = new MySQLIndex();
                idx.setName(keyName);
                idx.setUnique(unique);
                idx.setColumns(new LinkedList<>(Collections.singleton(columnName)));
                indices.add(idx);
            }
        });
        return indices;
    }

    @Override
    public List<Row> query(String query, long offset, long limit) {
        return query(query + " limit " + offset + ", " + limit);
    }



    public static void main(String[] args) {
        ConnectionProperties properties = new ConnectionProperties();
        properties.setUrl("jdbc:mysql://localhost:3306/?useUnicode=true&characterEncoding=utf8&serverTimezone=Asia/Shanghai");
        properties.setDriver("com.mysql.cj.jdbc.Driver");
        properties.setUser("root");
        properties.setPassword("123456");
        RdbmsServer server = RdbmsServerFactory.getServer(properties);

        List<Column> columns = server.columnsOf("select * from world.country");

        columns.forEach(System.out::println);

    }

}
