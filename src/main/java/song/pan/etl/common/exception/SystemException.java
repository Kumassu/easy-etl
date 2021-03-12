package song.pan.etl.common.exception;

public class SystemException extends GeneralException {


    public SystemException() {
        super(ErrorType.SYSTEM_ERROR.getCode());
    }

    public SystemException(String msg) {
        super(ErrorType.SYSTEM_ERROR.getCode(), msg);
    }

    public SystemException(Throwable e) {
        super(ErrorType.SYSTEM_ERROR.getCode(), e);
    }



}
