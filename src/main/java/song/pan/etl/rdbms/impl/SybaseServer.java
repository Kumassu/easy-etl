package song.pan.etl.rdbms.impl;

import song.pan.etl.rdbms.AbstractRdbmsServer;
import song.pan.etl.rdbms.RdbmsType;
import song.pan.etl.rdbms.element.Column;
import song.pan.etl.rdbms.element.Row;
import song.pan.etl.rdbms.element.Table;

import java.util.List;

/**
 * @author Song Pan
 * @version 1.0.0
 */
public class SybaseServer extends AbstractRdbmsServer {


    @Override
    public RdbmsType getType() {
        return RdbmsType.SYBASE;
    }


    @Override
    public Table fromFullQualifiedName(String name) {
        return null;
    }

    @Override
    public String fullQualifiedNameOf(Table table) {
        return null;
    }

    @Override
    public List<String> getCatalogs() {
        return null;
    }

    @Override
    public boolean isTableExist(Table table) {
        return false;
    }

    @Override
    public void renameTable(Table old, String newName) {

    }

    @Override
    public void copyToGenerateId(String query, Table to, String uid) {

    }

    @Override
    public String format(Object value) {
        return null;
    }

    @Override
    public String format(Column column) {
        return null;
    }

    @Override
    public List<Row> topRowsOf(String query, int num) {
        return null;
    }

    @Override
    public List<Column> columnsOf(Table table) {
        return null;
    }

}
