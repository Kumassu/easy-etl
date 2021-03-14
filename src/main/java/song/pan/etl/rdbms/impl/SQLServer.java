package song.pan.etl.rdbms.impl;

import song.pan.etl.rdbms.RdbmsType;

/**
 * @author Song Pan
 * @version 1.0.0
 */
public class SQLServer extends SybaseServer {


    @Override
    public RdbmsType getType() {
        return RdbmsType.SQL_SERVER;
    }

}
