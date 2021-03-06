package song.pan.etl.rdbms.element;

import lombok.Getter;
import lombok.Setter;

import java.util.List;

/**
 * @author Song Pan
 * @version 1.0.0
 */
@Getter
@Setter
public class Index {

    private String name;

    private List<String> columns;

    private boolean unique;

}
