package org.opencb.hpg.bigdata.analysis.tools;

import org.junit.Test;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.hpg.bigdata.analysis.exceptions.AnalysisToolException;

import java.net.URISyntaxException;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * Created by pfurio on 23/05/17.
 */
public class ToolManagerTest {

    @Test
    public void testToolManager() throws URISyntaxException, AnalysisToolException {
        Path toolPath = Paths.get(getClass().getResource("/tools").toURI());
        Path inputFile = Paths.get(getClass().getResource("/manifest-test.yml").toURI());

        ToolManager toolManager = new ToolManager(toolPath);
        ObjectMap params = new ObjectMap()
                .append("input", inputFile.toString())
                .append("o", inputFile.toString())
                .append("b", true)
                .append("S", true);

        System.out.println(toolManager.createCommandLine("samtools", "view", params));
    }

}