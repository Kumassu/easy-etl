package song.pan.etl.rdbms.element;

import lombok.Getter;
import lombok.Setter;

/**
 * @author Song Pan
 * @version 1.0.0
 */
@Getter
@Setter
public class Column {

    private String name;

    private String type;

    private Integer typeIndex;

    private int length;

    private Boolean nullable;

    private Object defaultValue;

    public Column() {
    }

    public Column(String name, String type) {
        this.name = name;
        this.type = type;
    }

    public Column(String name, String type, Integer typeIndex) {
        this.name = name;
        this.type = type;
        this.typeIndex = typeIndex;
    }


    public Column(String name, String type, Integer typeIndex, int length) {
        this.name = name;
        this.type = type;
        this.typeIndex = typeIndex;
        this.length = length;
    }

}
