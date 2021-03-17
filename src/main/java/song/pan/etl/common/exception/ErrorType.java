package song.pan.etl.common.exception;

import lombok.Getter;

@Getter
public enum ErrorType {


    ILLEGAL_ARGUMENT(401),
    INVALID_SQL(401),
    LOGIN_FAIL(402),
    DISCONNECT_FAIL(403),
    INCORRECT_LOAD_ROW(501),
    SYSTEM_ERROR(500),
    ;


    private int code;
    private String description;

    ErrorType(int code, String description) {
        this.code = code;
        this.description = description;
    }

    ErrorType(int code) {
        this.code = code;
    }
}
