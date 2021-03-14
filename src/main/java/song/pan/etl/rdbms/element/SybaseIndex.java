package song.pan.etl.rdbms.element;

import lombok.Getter;
import lombok.Setter;

/**
 * @author Song Pan
 * @version 1.0.0
 */
@Getter
@Setter
public class SybaseIndex extends Index {

    private boolean clustered;

}
