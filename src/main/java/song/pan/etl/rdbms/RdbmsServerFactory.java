package song.pan.etl.rdbms;

import song.pan.etl.common.exception.SystemException;

/**
 * @author Song Pan
 * @version 1.0.0
 */
public class RdbmsServerFactory {


    public static RdbmsServer getServer(ConnectionProperties properties) {
        RdbmsType rdbmsType = RdbmsType.deduce(properties.getUrl());

        if (null == rdbmsType) {
            throw new UnsupportedOperationException("Unknown url: " + properties.getUrl());
        }

        try {
            RdbmsServer rdbmsServer = rdbmsType.getClazz().getConstructor().newInstance();
            rdbmsServer.init(properties);
            return rdbmsServer;
        } catch (Exception e) {
            throw new SystemException(e);
        }
    }



}
