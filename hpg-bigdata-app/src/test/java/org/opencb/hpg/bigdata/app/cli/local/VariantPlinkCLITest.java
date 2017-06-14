package org.opencb.hpg.bigdata.app.cli.local;

import org.apache.htrace.fasterxml.jackson.databind.ObjectMapper;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.*;
import org.junit.Test;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.hpg.bigdata.analysis.variant.VariantAnalysisUtils;
import org.opencb.hpg.bigdata.core.lib.SparkConfCreator;
import org.opencb.hpg.bigdata.core.lib.VariantDataset;

import java.net.URISyntaxException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Iterator;

/**
 * Created by joaquin on 1/19/17.
 */
public class VariantPlinkCLITest {
    private Path inPath;
    private Path outPath;
    private Path confPath;

    private void init() throws URISyntaxException {
        String root = "/home/jtarraga/data150/test/assoc";
        inPath = Paths.get(root + "/test.vcf.avro");
        outPath = Paths.get(root);
    }

    @Test
    public void plink() {
        try {
            init();

            StringBuilder commandLine = new StringBuilder();
            commandLine.append(" variant plink");
            commandLine.append(" --log-level ERROR");
            commandLine.append(" -i ").append(inPath);
            commandLine.append(" -m ").append(inPath + ".meta.json");
            commandLine.append(" -o ").append(outPath);
            commandLine.append(" --dataset noname");
            //commandLine.append(" --region 22:16050100-17000000");
            //commandLine.append(" --plink-params pca");
            commandLine.append(" --plink-params toto");

            VariantQueryCLITest.execute(commandLine.toString());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void bean() throws Exception {
        init();

        SparkConf sparkConf = SparkConfCreator.getConf("variant plink", "local", 1, true);
        SparkSession sparkSession = new SparkSession(new SparkContext(sparkConf));
        VariantDataset vd = new VariantDataset(sparkSession);

        vd.load(inPath.toString());
        vd.createOrReplaceTempView("vcf");
        String sql = "select * from vcf";
        Dataset<Row> result = vd.sqlContext().sql(sql);


        JavaPairRDD<String, Iterable<Row>> rdd = result.toJavaRDD()
                .groupBy(row -> row.getString(row.fieldIndex("chromosome")));
        System.out.println(rdd.collect());
/*
        Encoder encoder = null;
        Encoder<Variant> variantEncoder = Encoders.bean(Variant.class);
        Dataset<Variant> dv = result.flatMap(new FlatMapFunction<Row, Variant>() {
            @Override
            public Iterator<Variant> call(Row row) throws Exception {
                return null;
            }
        }, variantEncoder).as(variantEncoder);

        String json = result.toJSON().head();
        ObjectMapper mapper = new ObjectMapper();
        Variant variant = mapper.readValue(json, Variant.class);
        System.out.println(variant.toString());
        System.out.println(variant.toJson());
*/
    }

}
