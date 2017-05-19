package org.opencb.hpg.bigdata.tools;

/**
 * Created by jtarraga on 30/01/17.
 */
public class ToolExecutorException extends Exception {

    public ToolExecutorException() {
    }

    public ToolExecutorException(String msg) {
        super(msg);
    }

    public ToolExecutorException(Exception e) {
        super(e);
    }

    public ToolExecutorException(String msg, Exception e) {
        super(msg, e);
    }

    public ToolExecutorException(String message, Throwable cause) {
        super(message, cause);
    }

    public ToolExecutorException(Throwable cause) {
        super(cause);
    }

    public ToolExecutorException(String message, Throwable cause,
                                 boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
