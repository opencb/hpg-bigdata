package org.opencb.hpg.bigdata.core.config;

import java.util.HashMap;
import java.util.Map;

public class AnalysisConfiguration {

    // Map for binary paths, e.g.: (key, value) = ("plink", "~/softs/plink")
    private Map<String, String> binaries;

    public AnalysisConfiguration() {
        this(new HashMap<>());
    }

    public AnalysisConfiguration(Map<String, String> binaries) {
        this.binaries = binaries;
    }

    @Override
    public String toString() {
        return "AnalysisConfiguration{"
                + "binaries=" + binaries + '}';
    }

    public Map<String, String> getBinaries() {
        return binaries;
    }

    public void setBinaries(Map<String, String> binaries) {
        this.binaries = binaries;
    }
}
