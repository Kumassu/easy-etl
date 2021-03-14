package song.pan.etl.rdbms.element;

import lombok.Getter;
import lombok.Setter;

/**
 * @author Song Pan
 * @version 1.0.0
 */
@Getter
@Setter
public class DataType<T> {


    private int typeIndex;

    private String typeName;

    private Class<T> cls;

    public static <T> DataType<T> of(int index, String name, Class<T> cls) {
        DataType<T> dataType = new DataType<>();
        dataType.typeIndex = index;
        dataType.typeName = name;
        dataType.cls = cls;
        return dataType;
    }





}
