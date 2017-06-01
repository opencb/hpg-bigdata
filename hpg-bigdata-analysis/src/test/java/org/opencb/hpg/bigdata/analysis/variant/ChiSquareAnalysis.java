package org.opencb.hpg.bigdata.analysis.variant;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.mllib.linalg.Matrices;
import org.apache.spark.mllib.linalg.Matrix;
import org.apache.spark.mllib.stat.Statistics;
import org.apache.spark.mllib.stat.test.ChiSqTestResult;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.opencb.biodata.models.core.pedigree.Individual;
import org.opencb.biodata.models.core.pedigree.Pedigree;
import org.opencb.hpg.bigdata.analysis.variant.statistics.MultipleTestCorrection;
import scala.collection.mutable.WrappedArray;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Created by jtarraga on 11/01/17.
 */
public class ChiSquareAnalysis {

    public static void run(Dataset<Row> data, Pedigree pedigree) {
        data.show();

        System.out.println(pedigree);
        List<Individual.Phenotype> phenotype = new ArrayList<>(pedigree.getIndividuals().size());
        for (Individual indivual: pedigree.getIndividuals().values()) {
            phenotype.add(indivual.getPhenotype());
        }

        System.out.println(phenotype);

        //result.show();
        Encoder<ChiSquareAnalysisResult> resultEncoder = Encoders.bean(ChiSquareAnalysisResult.class);
        Dataset<ChiSquareAnalysisResult> resultDS = data.map(new MapFunction<Row, ChiSquareAnalysisResult>() {
            @Override
            public ChiSquareAnalysisResult call(Row r) throws Exception {

                double[] counters = new double[]{0, 0, 0, 0};

                System.out.print("Variant at " + r.get(1) + ":" + r.get(2) + " -> ");
                for (int i = 0; i < phenotype.size(); i++) {
                    final int length = 2;
                    String pheno = ((WrappedArray) r.getList(5).get(i)).head().toString();
                    System.out.print(pheno + "\t");

                    String[] fields = pheno.split("[|/]");
                    if (fields.length == length) {
                        if (phenotype.get(i) != Individual.Phenotype.MISSING) {
                            for (int j = 0; j < length; j++) {
                                switch (fields[j]) {
                                    case "0":
                                        counters[phenotype.get(i) == Individual.Phenotype.UNAFFECTED ? 0 : 2]++;
                                        break;
                                    case "1":
                                        counters[phenotype.get(i) == Individual.Phenotype.UNAFFECTED ? 1 : 3]++;
                                        break;
                                }
                            }
                        }
                    }
                }
                System.out.print("\tmatrix: ");
                for (int i = 0; i < counters.length; i++) {
                    System.out.print(counters[i] + "\t");
                }
                System.out.println("");

                Matrix mat = Matrices.dense(2, 2, counters);
                ChiSqTestResult independenceTestResult = Statistics.chiSqTest(mat);

                ChiSquareAnalysisResult res = new ChiSquareAnalysisResult();
                res.setStatistic(independenceTestResult.statistic());
                res.setpValue(independenceTestResult.pValue());
                res.setDegreesOfFreedom(independenceTestResult.degreesOfFreedom());
                res.setMethod(independenceTestResult.method());
                res.setAdjPValue(0.0);
                return res;
            }
        }, resultEncoder);
        resultDS.show(false);


        double[] pValues = new double[(int) resultDS.count()];

        int i = 0;
        Iterator<Row> it = resultDS.toDF().javaRDD().toLocalIterator();
        while (it.hasNext()) {
            Row row = it.next();
            pValues[i++] = row.getDouble(row.fieldIndex("pValue"));
        }

        System.out.print("pValues = ");
        for (i = 0; i < pValues.length; i++) {
            System.out.print(pValues[i] + "\t");
        }
        System.out.println("");

        MultipleTestCorrection multipleTestCorrection = new MultipleTestCorrection();
        double[] adjPValues = multipleTestCorrection.fdrCorrection(pValues);

        System.out.print("adj. pValues = ");
        for (i = 0; i < adjPValues.length; i++) {
            System.out.print(adjPValues[i] + "\t");
        }
        System.out.println("");

        //resultDS.map

        //resultDS.coalesce(1).write().format("json").save("/tmp/test.vcf.chi.square");

//        it = resultDS.toDF().javaRDD().toLocalIterator();
//        while (it.hasNext()) {
//            Row row = it.next();
//            row. getDouble(row.fieldIndex("pValue"));
//        }

//        resultDS.show(false);

//        System.out.println("column pValue");
//        System.out.println(resultDS.col("pValue"));

//val mkString = udf((a: Seq[Double]) => a.mkString(", "))
//df.withColumn("coordinates_string", mkString($"coordinates"))

//        resultDS.withColumn("adj. pValue", new Column(adj));
    }
}
