package song.pan.etl.common.exception;


public class TableConflictException extends IllegalArgumentException {

    public TableConflictException() {

    }

    public TableConflictException(String msg) {
        super(msg);
    }

    public TableConflictException(Throwable e) {
        super(e);
    }

}
