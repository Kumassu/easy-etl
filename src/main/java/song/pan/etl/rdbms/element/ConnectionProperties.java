package song.pan.etl.rdbms.element;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class ConnectionProperties {
    private String name;
    private String urls;
    private String driver;
    private String user;
    private String password;
    private Boolean decryptPassword;
    private int maxPoolSize;
}