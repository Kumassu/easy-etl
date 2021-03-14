package song.pan.etl.rdbms.element;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * @author Song Pan
 * @version 1.0.0
 */
public class Row {

    Map<String, Object> data;

    public Row() {
        data = new HashMap<>();
    }

    Row(Map<String, Object> data) {
        this.data = data;
    }

    public static Row fromMap(Map<String, Object> data) {
        return new Row(data);
    }

    public void addAll(Map<String, Object> data) {
        data.putAll(data);
    }

    public Object getColumn(String column) {
        return data.get(column);
    }

    public void setColumn(String column, Object value) {
        data.put(column, value);
    }

    public Set<Map.Entry<String, Object>> entrySet() {
        return data.entrySet();
    }


    @Override
    public String toString() {
        return data.toString();
    }
}
