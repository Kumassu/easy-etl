package song.pan.etl.rdbms;

import com.zaxxer.hikari.HikariDataSource;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.jdbc.core.JdbcTemplate;
import song.pan.etl.common.exception.IllegalArgumentException;
import song.pan.etl.common.exception.SystemException;
import song.pan.etl.common.util.AESUtils;
import song.pan.etl.config.AppSetting;
import song.pan.etl.rdbms.element.*;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSetMetaData;
import java.util.*;
import java.util.stream.Collectors;

/**
 * @author Song Pan
 * @version 1.0.0
 */
@Getter
@Setter
@Slf4j
public abstract class AbstractRdbmsServer implements RdbmsServer {


    private DataSource dataSource;


    private ConnectionProperties connectionProperties;


    @Override
    public boolean isServerAlive() {
        try {
            connect();
        } catch (Exception e) {
            log.info("Server not alive for: {} : {}", e.getClass().getName(), e.getMessage());
            return false;
        }
        return true;
    }

    @Override
    public boolean isDatabaseAvailable(String name) {
        return isServerAlive();
    }


    /**
     * Use {@link HikariDataSource} as default data source,
     * child implementations could override it to customize.
     *
     * @return well feed data source
     */
    protected DataSource createDataSource() {
        HikariDataSource dataSource = new HikariDataSource();
        dataSource.setDriverClassName(connectionProperties.getDriver());
        dataSource.setJdbcUrl(connectionProperties.getUrl());
        dataSource.setUsername(connectionProperties.getUser());
        dataSource.setPassword(connectionProperties.getPassword());

        if (connectionProperties.getMaxPoolSize() > 0) {
            dataSource.setMaximumPoolSize(connectionProperties.getMaxPoolSize());
        }
        return dataSource;
    }

    @Override
    public DataSource getDataSource() {
        synchronized (AbstractRdbmsServer.class) {
            if (null == dataSource) {
                dataSource = createDataSource();
            }
        }
        return dataSource;
    }

    @Override
    public JdbcTemplate getJdbcTemplate() {
        return new JdbcTemplate(getDataSource());
    }

    @Override
    public void init(ConnectionProperties properties) {
        setConnectionProperties(properties);

        // decrypt password
        if (Boolean.TRUE.equals(properties.getDecryptPassword())) {
            properties.setPassword(AESUtils.decrypt(AppSetting.AES_SEED, properties.getPassword()));
        }

        // ensure driver
        if (null == properties.getDriver()) {
            properties.setDriver(getType().getDefaultDriver());
        }
        try {
            Class.forName(properties.getDriver());
        } catch (ClassNotFoundException e) {
            throw new UnsupportedOperationException("Driver class not found: " + properties.getDriver());
        }

        // try connect
        connect();
    }

    @Override
    public void connect() {
        getJdbcTemplate().queryForObject("SELECT 1", Integer.class);
    }

    @Override
    public void disconnect() {
        log.info("[{}] Disconnect from {}", getType(), this);
        Optional.ofNullable(dataSource).ifPresent(ds -> {
            if (ds instanceof HikariDataSource) {
                ((HikariDataSource) ds).close();
            }
        });
    }


    @Override
    public String toString() {
        return null == connectionProperties ? super.toString() : connectionProperties.getName();
    }


    @Override
    public void createTable(Table table) {
        getJdbcTemplate().update("CREATE TABLE " + fullQualifiedNameOf(table) + "(" +
                table.getColumns().stream().map(this::format).collect(Collectors.joining(",")) + ")");
    }


    @Override
    public void truncateTable(Table table) {
        getJdbcTemplate().update("TRUNCATE TABLE " + fullQualifiedNameOf(table));
    }


    @Override
    public void copyTable(Table from, Table to) {
        String joinedNames = to.getColumns().stream().map(Column::getName).collect(Collectors.joining(","));
        getJdbcTemplate().update("INSERT INTO" + fullQualifiedNameOf(to) + "(" + joinedNames + ") " +
                " SELECT " + joinedNames + " FROM " + fullQualifiedNameOf(from));
    }


    @Override
    public void dropTable(Table table) {
        getJdbcTemplate().update("DROP TABLE " + fullQualifiedNameOf(table));
    }

    @Override
    public long count(String query) {
        Long count = getJdbcTemplate().queryForObject("SELECT COUNT(1) FROM (" + query + ") t", Long.class);
        if (null == count) {
            throw new SystemException("Count query return null : " + query);
        }
        return count;
    }


    @Override
    public long execute(String sql) {
        return getJdbcTemplate().update(sql);
    }


    @Override
    public long execute(String catalog, String sql) {
        JdbcTemplate jdbcTemplate = getJdbcTemplate();
        jdbcTemplate.update("USE " + catalog);
        return jdbcTemplate.update(sql);
    }


    @Override
    public List<Column> columnsOf(String query) {
        LinkedList<Column> columns = new LinkedList<>();
        try (Connection connection = getDataSource().getConnection();
             PreparedStatement statement = connection.prepareStatement("SELECT * FROM (" + query + ") tbl")) {
            statement.execute();
            ResultSetMetaData metaData = statement.getMetaData();
            for (int i = 1; i <= metaData.getColumnCount(); i++) {
                Column column = new Column();
                column.setName(metaData.getColumnName(i));
                column.setType(metaData.getColumnTypeName(i));
                column.setLength(metaData.getColumnDisplaySize(i));
                column.setScale(metaData.getScale(i));
                column.setPrecision(metaData.getPrecision(i));
                column.setNullable(metaData.isNullable(i) == 1);
                column.setTypeIndex(typeOf(column.getType()).getTypeIndex());
                columns.add(column);
            }
        } catch (Exception e) {
            throw new SystemException((e));
        }
        return columns;
    }

    @Override
    public List<String> primaryKeyOf(Table table) {
        return Collections.emptyList();
    }

    @Override
    public List<Index> indexesOf(Table table) {
        return Collections.emptyList();
    }

    @Override
    public Object maxOf(String query, String column) {
        List<Map<String, Object>> rows = getJdbcTemplate().queryForList("SELECT MAX(" + column + ") MAX_V FROM (" + query + ") t");
        return rows.isEmpty() ? null : rows.get(0).get("MAX_V");
    }

    @Override
    public Object minOf(String query, String column) {
        List<Map<String, Object>> rows = getJdbcTemplate().queryForList("SELECT MIN(" + column + ") MIN_V FROM (" + query + ") t");
        return rows.isEmpty() ? null : rows.get(0).get("MIN_V");
    }

    @Override
    public Row lastRowOf(String query, List<String> columns) {
        Row lastRow = new Row();
        for (int i = 0; i < columns.size(); i++) {
            String ci = columns.get(i);
            StringBuilder findMax = new StringBuilder("SELECT MAX(").append(ci).append(") FROM (").append(query).append(") t");
            if (i != 0) {
                int count = 0;
                for (Map.Entry<String, Object> entry : lastRow.entrySet()) {
                    if (count++ == 0) {
                        findMax.append(" WHERE ");
                    } else {
                        findMax.append(" AND ");
                    }
                    findMax.append(entry.getKey()).append(" = ").append(format(entry.getValue()));
                }
            }
            Object max = getJdbcTemplate().queryForObject(findMax.toString(), Object.class);
            lastRow.setColumn(ci, max);
        }
        return lastRow;
    }

    @Override
    public List<Row> query(String query, Column column, Object startBoundary, Object endBoundary) {
        return query("SELECT * FROM (" + query + ") t WHERE t." + column.getName() +
                        " >= " + format(startBoundary) + " AND t." + column.getName() +
                        " <= " + format(endBoundary));
    }

    @Override
    public List<Row> query(String query, long offset, long limit) {
        throw new UnsupportedOperationException();
    }

    @Override
    public List<Row> query(String query) {
        return getJdbcTemplate().queryForList(query).stream().map(Row::fromMap).collect(Collectors.toList());
    }

    @Override
    public String prepareInsertStatement(Table table) {
        return "INSERT INTO " + fullQualifiedNameOf(table) + "(" +
                table.getColumns().stream().map(Column::getName).collect(Collectors.joining(",")) +
                ") VALUES (" +
                table.getColumns().stream().map(e -> "?").collect(Collectors.joining(",")) +
                ")";
    }

    @Override
    public String prepareNamedInsertStatement(Table table) {
        return "INSERT INTO " + fullQualifiedNameOf(table) + "(" +
                table.getColumns().stream().map(Column::getName).collect(Collectors.joining(",")) +
                ") VALUES (" +
                table.getColumns().stream().map(e -> ":" + e.getName()).collect(Collectors.joining(",")) +
                ")";
    }

    @Override
    public String select(Table table) {
        List<Column> columns = table.getColumns();
        if (null == columns || columns.isEmpty()) {
            return "SELECT * FROM " + fullQualifiedNameOf(table);
        }
        return "SELECT " + columns.stream().map(Column::getName).collect(Collectors.joining(",")) +
                " FROM " + fullQualifiedNameOf(table);
    }


    protected List<DataType> getAllDataTypes() {
        throw new UnsupportedOperationException();
    }


    @Override
    public DataType typeOf(String typeName) {
        return getAllDataTypes().stream()
                .filter(type -> type.getTypeName().equalsIgnoreCase(typeName))
                .findAny().orElse(null);
    }

    @Override
    public DataType typeOf(int typeIndex) {
        return getAllDataTypes().stream()
                .filter(type -> type.getTypeIndex() == typeIndex)
                .findAny().orElse(null);
    }


    protected void setTypeIndex(Column column) {
        DataType dataType = typeOf(column.getType());
        if (null == dataType) {
            throw new IllegalArgumentException("Type not support: " + column.getType() + ", column: " + column.getName());
        }
        column.setTypeIndex(dataType.getTypeIndex());
    }


}
