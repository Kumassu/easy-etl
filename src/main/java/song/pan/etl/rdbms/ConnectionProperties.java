package song.pan.etl.rdbms;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class ConnectionProperties {
    private String name;
    private String url;
    private String driver;
    private String user;
    private String password;
    private Boolean decryptPassword;
    private int maxPoolSize;
}