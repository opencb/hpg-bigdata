package org.opencb.hpg.bigdata.analysis.tools.manifest;

/**
 * Created by pfurio on 01/06/17.
 */
public class Settings {

    /**
     * String indicating the separator used between every parameter to build the command line --key value, --key=value.
     * Typically ' ' or '='.
     */
    private String separator;

    /**
     * String pointing to the shell script that will be executed before running any execution to load the necessary environment variables.
     */
    private String env;

    /**
     * Flag indicating whether the parameters follow the POSIX command line standard.
     * If true, we will assume the parameters are preceded by '-' if it is a one letter parameter or '--' otherwise.
     */
    private boolean posix;

    public Settings() {
    }

    public Settings(String separator, String env, boolean posix) {
        this.separator = separator;
        this.env = env;
        this.posix = posix;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("Settings{");
        sb.append("separator='").append(separator).append('\'');
        sb.append(", env='").append(env).append('\'');
        sb.append(", posix=").append(posix);
        sb.append('}');
        return sb.toString();
    }

    public String getSeparator() {
        return separator;
    }

    public Settings setSeparator(String separator) {
        this.separator = separator;
        return this;
    }

    public String getEnv() {
        return env;
    }

    public Settings setEnv(String env) {
        this.env = env;
        return this;
    }

    public boolean isPosix() {
        return posix;
    }

    public Settings setPosix(boolean posix) {
        this.posix = posix;
        return this;
    }
}
