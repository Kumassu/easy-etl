package song.pan.etl.service.domain;

import lombok.Getter;
import lombok.Setter;
import song.pan.etl.rdbms.element.Row;

import java.util.List;

/**
 * @author Song Pan
 * @version 1.0.0
 */
@Getter
@Setter
public class Page {

    private long index;
    private List<Row> data;
    private long extractTimeMs;
    private long loadTimeMs;

    public Page(List<Row> data) {
        this.data = data;
    }

    public Page(long index) {
        this.index = index;
    }

    public Page(long index, List<Row> data) {
        this.index = index;
        this.data = data;
    }
}
