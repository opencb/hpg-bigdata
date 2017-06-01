package org.opencb.hpg.bigdata.analysis.exceptions;

/**
 * Created by pfurio on 23/05/17.
 */
public class AnalysisToolException extends AnalysisException {

    public AnalysisToolException(String message) {
        super(message);
    }

    public AnalysisToolException(String message, Throwable cause) {
        super(message, cause);
    }

    public AnalysisToolException(Throwable cause) {
        super(cause);
    }
}
