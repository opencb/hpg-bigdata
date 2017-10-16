package org.opencb.hpg.bigdata.core.config;

import org.junit.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.nio.file.Paths;
import java.util.Arrays;

public class OskarConfigurationTest {

    @Test
    public void testDefault() throws Exception {
        OskarConfiguration storageConfiguration = new OskarConfiguration();

//
        CellBaseConfiguration cellBaseConfiguration = new CellBaseConfiguration(Arrays.asList("localhost"), "v3", new DatabaseCredentials
                (Arrays.asList("localhost"), "user", "password"));

        storageConfiguration.setCellbase(cellBaseConfiguration);
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