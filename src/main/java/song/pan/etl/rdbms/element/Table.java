package song.pan.etl.rdbms.element;

import lombok.Getter;
import lombok.Setter;

import java.util.LinkedList;
import java.util.List;

/**
 * @author Song Pan
 * @version 1.0.0
 */
@Getter
@Setter
public class Table {


    private String catalog;
    private String name;
    private String schema;
    private List<Column> columns;


    public Table() {
        columns = new LinkedList<>();
    }

    public Table(String name) {
        this.name = name;
    }

    public Table(String catalog, String name) {
        this.catalog = catalog;
        this.name = name;
    }

    public Table(String catalog, String schema, String name) {
        this.catalog = catalog;
        this.name = name;
        this.schema = schema;
    }

    public Table(String catalog, String name, List<Column> columns) {
        this.catalog = catalog;
        this.name = name;
        this.columns = columns;
    }

    public Table(String catalog, String name, String schema, List<Column> columns) {
        this.catalog = catalog;
        this.name = name;
        this.schema = schema;
        this.columns = columns;
    }
}
