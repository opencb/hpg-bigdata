package org.opencb.hpg.bigdata.analysis.exceptions;

/**
 * Created by jtarraga on 30/01/17.
 */
public class AnalysisExecutorException extends Exception {

    public AnalysisExecutorException() {
    }

    public AnalysisExecutorException(String msg) {
        super(msg);
    }

    public AnalysisExecutorException(Exception e) {
        super(e);
    }

    public AnalysisExecutorException(String msg, Exception e) {
        super(msg, e);
    }

    public AnalysisExecutorException(String message, Throwable cause) {
        super(message, cause);
    }

    public AnalysisExecutorException(Throwable cause) {
        super(cause);
    }

    public AnalysisExecutorException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
