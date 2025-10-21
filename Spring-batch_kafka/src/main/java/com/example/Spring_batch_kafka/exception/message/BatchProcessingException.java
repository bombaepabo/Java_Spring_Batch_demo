package com.example.Spring_batch_kafka.exception.message;

public class BatchProcessingException extends RuntimeException {

    private String errorCode;
    private Object details;

    public BatchProcessingException(String message) {
        super(message);
    }

    public BatchProcessingException(String message, Throwable cause) {
        super(message, cause);
    }

    public BatchProcessingException(String message, String errorCode) {
        super(message);
        this.errorCode = errorCode;
    }

    public BatchProcessingException(String message, String errorCode, Throwable cause) {
        super(message, cause);
        this.errorCode = errorCode;
    }

    public BatchProcessingException(String message, String errorCode, Object details) {
        super(message);
        this.errorCode = errorCode;
        this.details = details;
    }

    public String getErrorCode() {
        return errorCode;
    }

    public Object getDetails() {
        return details;
    }
}
