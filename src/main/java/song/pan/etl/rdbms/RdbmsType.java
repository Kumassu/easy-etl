package song.pan.etl.rdbms;

import song.pan.etl.rdbms.impl.MySQLServer;
import song.pan.etl.rdbms.impl.SQLServer;
import song.pan.etl.rdbms.impl.SybaseServer;

import java.util.regex.Pattern;

/**
 * @author Song Pan
 * @version 1.0.0
 */
public enum RdbmsType {

    SYBASE(Pattern.compile("jdbc:sybase.*"), "com.sybase.jdbc4.jdbc.SybDriver", SybaseServer.class),
    SQL_SERVER(Pattern.compile("jdbc:sqlserver.*"), "com.microsoft.sqlserver.jdbc.SQLServerDriver", SQLServer.class),
    MYSQL(Pattern.compile("jdbc:mysql.*"), "com.mysql.cj.jdbc.Driver", MySQLServer.class),
    ;

    private Pattern urlPattern;
    private String defaultDriver;
    private Class<? extends RdbmsServer> clazz;

    RdbmsType(Pattern urlPattern, String defaultDriver, Class<? extends RdbmsServer> clazz) {
        this.urlPattern = urlPattern;
        this.defaultDriver = defaultDriver;
        this.clazz = clazz;
    }


    public static RdbmsType deduce(String url) {
        for (RdbmsType rdbmsType : values()) {
            if (rdbmsType.urlPattern.matcher(url).matches()) {
                return rdbmsType;
            }
        }
        return null;
    }
}
