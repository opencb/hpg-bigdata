package org.opencb.hpg.bigdata.core.config;

import java.util.HashMap;
import java.util.Map;

public class AnalysisConfiguration extends HashMap<String, AnalysisConfiguration.Analysis> {

    public static class Analysis {
        private String path;
        private String version;

        public Analysis() {
        }

        public Analysis(String path, String version) {
            this.path = path;
            this.version = version;
        }

        @Override
        public String toString() {
            return "Analysis{" + "path='" + path + '\'' + ", version='" + version + '\'' + '}';
        }

        public String getPath() {
            return path;
        }

        public Analysis setPath(String path) {
            this.path = path;
            return this;
        }

        public String getVersion() {
            return version;
        }

        public Analysis setVersion(String version) {
            this.version = version;
            return this;
        }
    }

    public AnalysisConfiguration() {
        super(new HashMap<>());
    }

    public AnalysisConfiguration(Map<String, Analysis> analysis) {
        super(analysis);
    }
}
