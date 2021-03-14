package song.pan.etl.common.exception;


public class InvalidSQLException extends IllegalArgumentException {

    public InvalidSQLException() {

    }

    public InvalidSQLException(String msg) {
        super(msg);
    }

    public InvalidSQLException(Throwable e) {
        super(e);
    }

}
