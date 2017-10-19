package org.opencb.hpg.bigdata.analysis.tools;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.Before;
import org.junit.Test;
import org.opencb.hpg.bigdata.analysis.exceptions.AnalysisToolException;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Created by pfurio on 23/05/17.
 */
public class ToolManagerTest {

    private Path tmpFolder;
    private Path toolPath;

    @Before
    public void setUp() throws Exception {
        tmpFolder = Paths.get("target/test-data/junit-" + RandomStringUtils.randomAlphabetic(10));
        Files.createDirectories(tmpFolder);

        toolPath = tmpFolder.resolve("tools");
        Files.createDirectories(toolPath.resolve("samtools"));

        Files.copy(Paths.get(getClass().getResource("/tools/samtools/manifest.json").toURI()), toolPath.resolve("samtools").resolve("manifest.json"));
        Files.copy(Paths.get(getClass().getResource("/tools/samtools/samtools").toURI()), toolPath.resolve("samtools").resolve("samtools"));

        toolPath.resolve("samtools").resolve("samtools").toFile().setExecutable(true);
    }

    @Test
    public void testToolManager() throws URISyntaxException, AnalysisToolException, IOException {
        Path testBam = Paths.get(getClass().getResource("/test.bam").toURI());

        ToolManager toolManager = new ToolManager(toolPath);
        Map<String, String> params = new HashMap<>();
        params.put("input", testBam.toAbsolutePath().toString());
        params.put("output", tmpFolder.resolve("test.bam.bai").toString());

        String commandLine = toolManager.createCommandLine("samtools", "index", params);
        System.out.println(commandLine);
        Executor.execute(commandLine, tmpFolder, true);

        ObjectReader reader = new ObjectMapper().reader(Status.class);
        Status status = reader.readValue(tmpFolder.resolve("status.json").toFile());
        assertEquals(Status.DONE, status.getName());

        assertTrue(tmpFolder.resolve("test.bam.bai").toFile().exists());
    }

}