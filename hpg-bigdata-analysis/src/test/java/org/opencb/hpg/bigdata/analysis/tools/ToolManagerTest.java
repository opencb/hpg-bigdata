package org.opencb.hpg.bigdata.analysis.tools;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import org.junit.Test;
import org.opencb.hpg.bigdata.analysis.exceptions.AnalysisToolException;

import java.io.IOException;
import java.net.URISyntaxException;
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

    @Test
    public void testToolManager() throws URISyntaxException, AnalysisToolException, IOException {
        Path toolPath = Paths.get(getClass().getResource("/tools").toURI());
        Path testBam = Paths.get(getClass().getResource("/test.bam").toURI());
        Path tmp = Paths.get("/tmp");

        ToolManager toolManager = new ToolManager(toolPath);
        Map<String, String> params = new HashMap<>();
        params.put("input", testBam.toAbsolutePath().toString());
        params.put("output", "/tmp/test.bam.bai");

        String commandLine = toolManager.createCommandLine("samtools", "index", params);
        System.out.println(commandLine);
        Executor.execute(commandLine, tmp, true);

        ObjectReader reader = new ObjectMapper().reader(Status.class);
        Status status = reader.readValue(tmp.resolve("status.json").toFile());
        assertEquals(Status.DONE, status.getName());

        assertTrue(tmp.resolve("test.bam.bai").toFile().exists());
    }

}