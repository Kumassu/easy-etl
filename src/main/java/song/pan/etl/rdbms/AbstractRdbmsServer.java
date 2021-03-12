package song.pan.etl.rdbms;

import lombok.Getter;
import lombok.Setter;
import org.springframework.jdbc.core.JdbcTemplate;
import song.pan.etl.rdbms.element.*;

import javax.sql.DataSource;
import java.util.List;

/**
 * @author Song Pan
 * @version 1.0.0
 */
@Getter
@Setter
public abstract class AbstractRdbmsServer implements RdbmsServer {


    private DataSource dataSource;


    private ConnectionProperties properties;


    @Override
    public boolean isServerAlive() {
        return false;
    }

    @Override
    public boolean isDatabaseAvailable(String name) {
        return false;
    }

    @Override
    public ConnectionProperties getConnectionProperties() {
        return null;
    }

    @Override
    public DataSource getDataSource() {
        return null;
    }

    @Override
    public JdbcTemplate getJdbcTemplate() {
        return null;
    }

    @Override
    public void init(ConnectionProperties properties) {

    }

    @Override
    public void connect() {

    }

    @Override
    public void disconnect() {

    }


    @Override
    public void createTable(Table table) {

    }

    @Override
    public void truncateTable(Table table) {

    }

    @Override
    public void copyTable(Table from, Table to, boolean truncate) {

    }

    @Override
    public void dropTable() {

    }

    @Override
    public long count(String query) {
        return 0;
    }

    @Override
    public void execute(String sql) {

    }

    @Override
    public void execute(String catalog, String sql) {

    }

    @Override
    public void validate(String query) {

    }

    @Override
    public List<Column> columnsOf(String query) {
        return null;
    }

    @Override
    public List<String> primaryKeyOf(Table table) {
        return null;
    }

    @Override
    public List<Index> indicesOf(Table table) {
        return null;
    }

    @Override
    public Object maxOf(String query, String column) {
        return null;
    }

    @Override
    public Object minOf(String query, String column) {
        return null;
    }

    @Override
    public Row lastRowOf(String query, List<String> orderBys) {
        return null;
    }

    @Override
    public List<Row> query(String query, Column column, Object startBoundary, Object endBoundary) {
        return null;
    }

    @Override
    public List<Row> query(String query, long offset, long limit) {
        return null;
    }

    @Override
    public List<Row> query(String query) {
        return null;
    }

    @Override
    public String prepareInsertStatement(Table table) {
        return null;
    }

    @Override
    public String prepareNamedInsertStatement(Table table) {
        return null;
    }

    @Override
    public String select(Table table) {
        return null;
    }


    protected List<DataType> getAllDataTypes() {
        throw new UnsupportedOperationException();
    }


    @Override
    public DataType typeOf(String typeName) {
        return null;
    }

    @Override
    public DataType typeOf(int typeIndex) {
        return null;
    }

}
