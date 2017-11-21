package org.opencb.hpg.bigdata.core.config;

import org.junit.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public class OskarConfigurationTest {

    @Test
    public void testDefault() throws Exception {
        OskarConfiguration storageConfiguration = new OskarConfiguration();

        CellBaseConfiguration cellBaseConfiguration = new CellBaseConfiguration(Arrays.asList("localhost"), "v3", new DatabaseCredentials
                (Arrays.asList("localhost"), "user", "password"));
        storageConfiguration.setCellbase(cellBaseConfiguration);

        AnalysisConfiguration.Analysis plink = new AnalysisConfiguration.Analysis("~/soft/plink", "1.9");
        AnalysisConfiguration.Analysis rvtests = new AnalysisConfiguration.Analysis("~/soft/rvtests", "2017");
        Map<String, AnalysisConfiguration.Analysis> analysisMap = new HashMap<>();
        analysisMap.put("plink", plink);
        analysisMap.put("rvtests", rvtests);
        storageConfiguration.setAnalysis(new AnalysisConfiguration(analysisMap));

        File file = Paths.get("/tmp/configuration.yml").toFile();
        try (FileOutputStream os = new FileOutputStream(file)) {
            storageConfiguration.serialize(os);
        }
        try (FileInputStream is = new FileInputStream(file)) {
            OskarConfiguration.load(is, "yml");
        }
    }

    @Test
    public void testLoad() throws Exception {
        OskarConfiguration storageConfiguration = OskarConfiguration.load(getClass().getResource("/configuration.yml")
                .openStream());
        System.out.println("oskarConfiguration = " + storageConfiguration);
    }
}