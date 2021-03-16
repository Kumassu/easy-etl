package song.pan.etl.rdbms.impl;

import lombok.Getter;
import lombok.Setter;
import song.pan.etl.rdbms.element.Index;

/**
 * @author Song Pan
 * @version 1.0.0
 */
@Getter
@Setter
public class SybaseIndex extends Index {

    private boolean clustered;

}
