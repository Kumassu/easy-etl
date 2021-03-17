package song.pan.etl.common.exception;


public class NoPaginationKeyException extends IllegalArgumentException {

    public NoPaginationKeyException() {

    }

    public NoPaginationKeyException(String msg) {
        super(msg);
    }

    public NoPaginationKeyException(Throwable e) {
        super(e);
    }

}
