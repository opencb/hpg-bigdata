package org.opencb.hpg.bigdata.analysis.tools.manifest;

import java.util.List;

/**
 * Created by pfurio on 23/05/17.
 */
public class ProgrammingLanguage {

    private String name;
    private String version;
    private List<ProgrammingLanguage> dependencies;

    public ProgrammingLanguage() {
    }

    public ProgrammingLanguage(String name, String version, List<ProgrammingLanguage> dependencies) {
        this.name = name;
        this.version = version;
        this.dependencies = dependencies;
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

    public List<ProgrammingLanguage> getDependencies() {
        return dependencies;
    }

    public ProgrammingLanguage setDependencies(List<ProgrammingLanguage> dependencies) {
        this.dependencies = dependencies;
        return this;
    }
}
