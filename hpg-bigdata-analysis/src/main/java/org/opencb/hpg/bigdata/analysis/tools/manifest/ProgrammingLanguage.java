package org.opencb.hpg.bigdata.analysis.tools.manifest;

/**
 * Created by pfurio on 23/05/17.
 */
public class ProgrammingLanguage {

    private String name;
    private String version;

    public ProgrammingLanguage() {
    }

    public ProgrammingLanguage(String name, String version) {
        this.name = name;
        this.version = version;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("ProgrammingLanguage{");
        sb.append("name='").append(name).append('\'');
        sb.append(", version='").append(version).append('\'');
        sb.append('}');
        return sb.toString();
    }

    public String getName() {
        return name;
    }

    public ProgrammingLanguage setName(String name) {
        this.name = name;
        return this;
    }

    public String getVersion() {
        return version;
    }

    public ProgrammingLanguage setVersion(String version) {
        this.version = version;
        return this;
    }

}
