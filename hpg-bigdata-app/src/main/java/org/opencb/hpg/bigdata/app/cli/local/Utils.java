package org.opencb.hpg.bigdata.app.cli.local;

import org.apache.commons.lang3.StringUtils;
import org.opencb.biodata.models.core.Region;
import org.opencb.commons.utils.FileUtils;
import org.opencb.hpg.bigdata.core.lib.ParentDataset;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by jtarraga on 12/09/16.
 */
public class Utils {

    public static String getOutputFilename(String input, String output, String to) throws IOException {
        String res = output;
        if (!res.isEmpty()) {
            Path parent = Paths.get(res).toAbsolutePath().getParent();
            if (parent != null) { // null if output is a file in the current directory
                FileUtils.checkDirectory(parent, true); // Throws exception, if does not exist
            }
        } else {
            res = input + "." + to;
        }
        return res;
    }

    public static List<Region> getRegionList(String regions, String regionFilename) throws IOException {
        List<Region> list = null;
        if (StringUtils.isNotEmpty(regions)) {
            list = Region.parseRegions(regions);
        }
        if (StringUtils.isNotEmpty(regionFilename) && new File(regionFilename).exists()) {
            if (regions == null) {
                list = new ArrayList<>();
            }
            List<String> lines = Files.readAllLines(Paths.get(regionFilename));
            for (String line : lines) {
                list.add(new Region(line));
            }
        }
        return list;
    }

    public static void saveDatasetAsOneAvroFile(ParentDataset ds, String filename) {
        String tmpDir = filename + ".tmp";
        ds.coalesce(1).write().format("com.databricks.spark.avro").save(tmpDir);

        File dir = new File(tmpDir);
        if (!dir.isDirectory()) {
            // error management
            System.err.println("Error: a directory was expected but " + tmpDir);
            return;
        }

        // list out all the file name and filter by the extension
        Boolean found = false;
        String[] list = dir.list();
        for (String name: list) {
            if (name.startsWith("part-r-") && name.endsWith("avro")) {
                new File(tmpDir + "/" + name).renameTo(new File(filename));
                found = true;
                break;
            }
        }
        if (!found) {
            // error management
            System.err.println("Error: pattern 'part-r-*avro' was not found");
            return;
        }
        dir.delete();
    }
}
