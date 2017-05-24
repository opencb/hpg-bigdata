package org.opencb.hpg.bigdata.analysis.tools;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Created by pfurio on 24/05/17.
 */

public class Status {

    public static final String RUNNING = "RUNNING";
    /**
     * DONE status means that the job has finished the execution, but the output is still not ready.
     */
    public static final String DONE = "DONE";
    /**
     * ERROR status means that the job finished with an error.
     */
    public static final String ERROR = "ERROR";

    private String name;
    private String date;
    private String message;

    public Status() {
        this(RUNNING, "");
    }

    public Status(String name) {
        this(name, "");
    }

    public Status(String name, String message) {
        this.name = name;
        this.date = getCurrentTime();
        this.message = message;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getDate() {
        return date;
    }

    public void setDate(String date) {
        this.date = date;
    }

    public void setCurrentDate() {
        this.date = getCurrentTime();
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("Status{");
        sb.append("name='").append(name).append('\'');
        sb.append(", date='").append(date).append('\'');
        sb.append(", message='").append(message).append('\'');
        sb.append('}');
        return sb.toString();
    }

    private String getCurrentTime() {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmss");
        return sdf.format(new Date());
    }

}