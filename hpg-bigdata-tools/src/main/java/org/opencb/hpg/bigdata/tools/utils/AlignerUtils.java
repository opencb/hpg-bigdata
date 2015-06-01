package org.opencb.hpg.bigdata.tools.utils;

import org.apache.avro.mapred.AvroKey;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.ga4gh.models.ReadAlignment;
import org.opencb.biodata.models.sequence.Read;
import org.opencb.hpg.bigdata.core.AlignerParams;
import org.opencb.hpg.bigdata.core.NativeAligner;
import org.opencb.hpg.bigdata.core.converters.SAMRecord2ReadAlignmentConverter;

import java.io.File;
import java.io.IOException;

/**
 * Created by jtarraga on 1/06/15.
 */
public class AlignerUtils {
    public static AlignerParams newAlignerParams(Configuration conf) {
        AlignerParams params = new AlignerParams();

        params.seqFileName1 = conf.get("seqFileName1");
        params.seqFileName2 = conf.get("seqFileName2");
        params.indexFolderName = conf.get("indexFolderName");
        params.numSeeds = conf.getInt("numSeeds", 0);
        params.minSWScore = conf.getFloat("minSWScore", 0.0f);

        System.out.println("conf.get(\"indexFolderName\") = " + conf.get("indexFolderName"));
        System.out.println("params.indexFolderName = " + params.indexFolderName);

        return params;
    }


    public void mapReads(String fastq, NativeAligner nativeAligner,
                          long nativeIndex, long nativeParams,
                          Mapper.Context context) throws IOException, InterruptedException {

        String sam = nativeAligner.map(fastq, nativeIndex, nativeParams);
        System.out.println("mapReads, sam:\n" + sam);

        String lines[] = sam.split("\n");

        ReadAlignment readAlignment;
        SAMRecord2ReadAlignmentConverter converter = new SAMRecord2ReadAlignmentConverter();

        for (String line: lines) {
            readAlignment = converter.forward(converter.backward2(line));
            System.out.println(readAlignment);
            context.write(new AvroKey<>(readAlignment), NullWritable.get());
        }
    }

    public void loadLibrary(String libname) {
        System.out.println("Loading library hpgaligner...");

        boolean loaded = false;
        String ld_library_path = System.getenv("LD_LIBRARY_PATH");
        String paths[] = ld_library_path.split(":");
        for(String path: paths) {
            if (new File(path + "/" + libname).exists()) {
                loaded = true;
                System.load(path + "/" + libname);
                break;
            }
        }
        if (!loaded) {
            System.out.println("Library " + libname + " not found. Set your environment variable: LD_LIBRARY_PATH library");
            System.out.println("Currently, LD_LIBRARY_PATH library = " + ld_library_path);
            System.exit(-1);
        }
        System.out.println("...done!");
    }
}
