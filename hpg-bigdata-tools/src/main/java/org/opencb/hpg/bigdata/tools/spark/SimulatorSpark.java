package org.opencb.hpg.bigdata.tools.spark;

import com.databricks.spark.avro.SchemaConverters;
import com.databricks.spark.avro.SchemaConverters$;
import org.apache.avro.Schema;
import org.apache.avro.generic.IndexedRecord;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.StructType;
import org.opencb.biodata.models.core.Region;
import org.opencb.biodata.models.variant.StudyEntry;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.avro.VariantAvro;
import org.opencb.biodata.models.variant.avro.VariantType;
import org.opencb.biodata.tools.variant.simulator.VariantSimulatorConfiguration;
import scala.Function1;

import java.io.Serializable;
import java.math.RoundingMode;
import java.text.DecimalFormat;
import java.util.*;

/**
 * Created by pawan on 08/02/16.
 */
public class SimulatorSpark implements Serializable{

    private Random rand = new Random();
    private static String referenceAllele = null;
    private static String alternateAllele = null;
    private static final int DEFAULT_NUM_SAMPLES = 10;

    private String inputFilePath = null;
    private String outputFilePath = null;

    private VariantSimulatorConfiguration variantSimulatorConfiguration;

    public SimulatorSpark() {
        this(new VariantSimulatorConfiguration());
    }

    public SimulatorSpark(VariantSimulatorConfiguration variantSimulatorConfigurationSpark) {
        this.variantSimulatorConfiguration = variantSimulatorConfigurationSpark;
        rand = new Random();
    }


    public Variant simulate() {
        return create(DEFAULT_NUM_SAMPLES, variantSimulatorConfiguration.getRegions());
    }


    public List<Variant> simulate(int numVariants) {
        List<Variant> variants = new ArrayList<>(numVariants);
        Set<Variant> createdVariants = new HashSet<>(numVariants);
        Variant variant;
        // TODO we need to control that there now variants in the same chromosome and start
//        for (int i = 0; i < numVariants; i++) {
//            variant = create(DEFAULT_NUM_SAMPLES, variantSimulatorConfiguration.getRegions());
//            variants.add(variant);
//        }
        variant = create(DEFAULT_NUM_SAMPLES, variantSimulatorConfiguration.getRegions());
        variants.add(variant);
        return variants;
    }


    public List<Variant> simulate(int numVariants, int numSamples, List<Region> regions) {
        numVariants = Math.max(numVariants, 1);
        numSamples = (numSamples <= 0) ? variantSimulatorConfiguration.getNumSamples() : numSamples;
        regions = (regions == null || regions.isEmpty()) ? variantSimulatorConfiguration.getRegions() : regions;

        List<Variant> variants = new ArrayList<>(numVariants);
        Variant variant;
//        for (int i = 0; i < numVariants; i++) {
//            variant = create(numSamples, regions);
//            variants.add(variant);
//        }
        variant = create(numSamples, regions);
        variants.add(variant);
        return variants;
    }


    public void variantSimulatorInputs(String input, String output, int numOfVariants, int numOfSamples, List<Region> regions) {
        inputFilePath = input;
        outputFilePath = output;
        simulate(numOfVariants, numOfSamples, regions);
    }

    /*public static class SimulatorRegistrator implements KryoRegistrator {
        public void registerClasses(Kryo kryo) {
            kryo.register(StudyEntry.class, new FieldSerializer(kryo, StudyEntry.class));
            kryo.register(VariantSimulatorConfiguration.class, new FieldSerializer(kryo, VariantSimulatorConfiguration.class));
            kryo.register(Variant.class, new FieldSerializer(kryo, Variant.class));
        }
    }*/




    private Variant create(int numSamples, List<Region> regions) {

        SparkConf sparkConf = new SparkConf().setAppName("Simulator").set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");

        /*sparkConf.set("spark.kryo.registrator", SimulatorRegistrator.class.getName());
        sparkConf.set("spark.kryo.registrator", VariantSimulatorConfiguration.class.getName());
        sparkConf.set("spark.kryo.registrator", Variant.class.getName());*/


        JavaSparkContext sc = new JavaSparkContext(sparkConf);

        JavaRDD<String> fileRDD = sc.textFile(inputFilePath);

        JavaRDD<String[]> rdd1 = fileRDD.map(line -> line.split("\\t"));

        JavaRDD<String> rdd2 = simulateVariants(rdd1);
//        for (String line : rdd2.collect()) {
//            System.out.println("variants = " + line);
//        }

        JavaRDD<String> rdd3 = addIndelAlleles(rdd2);
//        rdd3.collect().forEach(System.out::println);

        JavaRDD<String> rdd4 = addVariantType(rdd3);
//        rdd4.collect().forEach(System.out::println);

        JavaRDD<String> rdd5 = addId(rdd4);
//        rdd5.collect().forEach(System.out::println);

        Variant variant1 = new Variant();

        JavaRDD<Variant> variantJavaRDD = addStudies(DEFAULT_NUM_SAMPLES, rdd5);

        JavaRDD<VariantAvro> variantAvroJavaRDD = variantJavaRDD.map((Function<Variant, VariantAvro>) Variant::getImpl);

        /*VariantAvro variantAvro;
        for (Variant line : finalVariantRDD.collect()) {
            variantAvro = line.getImpl();
            System.out.println("variants = " + variantAvro);
        }*/


        Schema schema = SimulatorSpark.getAvroSchema(VariantAvro.class);
        //System.out.println("s = " + schema);
        DataFrame df = SimulatorSpark.createDataFrame(sc, variantAvroJavaRDD, schema);
        df.printSchema();
        df.registerTempTable("variant");

//        if (outputFilePath != null) {
//            df.write().format("com.databricks.spark.avro").mode("ignore").save(outputFilePath);
//        } else {
//            System.out.println("Output path is null or empty!!! ");
//        }

        sc.close();


        return variant1;
    }


    public static <T extends IndexedRecord> Schema getAvroSchema(Class clazz) {
        @SuppressWarnings("unchecked")
        T t = (T) ReflectionUtils.newInstance(clazz, null);
        return t.getSchema();
    }

    public static <T extends IndexedRecord> DataFrame createDataFrame(
            JavaSparkContext context, JavaRDD<T> rdd, Schema schema) {
        SQLContext sqlContext = new SQLContext(context);
        SchemaConverters.SchemaType schemaType = SchemaConverters$.MODULE$.toSqlType(schema);
        StructType structType = (StructType) schemaType.dataType();
        // Need to go through MODULE$ since SchemaConverters#createConverterToSQL is not
        // public. Note that https://github.com/databricks/spark-avro/pull/89 proposes to
        // make it public, but it's not going to be merged.
        final Function1<Object, Object> converter = SchemaConverters$.MODULE$.createConverterToSQL(schema);
        JavaRDD<Row> rows = rdd.map((Function<T, Row>) record -> (Row) converter.apply(record));
        return sqlContext.createDataFrame(rows, structType);
    }






    private JavaRDD<Variant> addStudies(int n, JavaRDD<String> variantRDD) {

        JavaRDD<Variant> variantRDD1 = variantRDD.map((Function<String, Variant>) s -> {

            int studyID = 2;
            int fieldID = 3;

            List<StudyEntry> studyEntryList = new ArrayList<>();
            StudyEntry studyEntry = new StudyEntry();
            studyEntry.setStudyId(Integer.toString(studyID));
            studyEntry.setFileId(Integer.toString(fieldID));
            Map<String, String> attributes = addAttributes();
            studyEntry.setAttributes(attributes);
            studyEntry.setFormat(getFormat());

            List<List<String>> sampleList = new ArrayList<>(getFormat().size());
            for (int i = 0; i < n; i++) {
                sampleList.add(getRandomample());
            }
            studyEntry.setSamplesData(sampleList);
            studyEntryList.add(studyEntry);

            //Create variant object and set the values
            Variant variant = new Variant();

            String[] parts = s.split("\\t");
            String strand = "+";

            variant.setChromosome(parts[0]);
            variant.setStart(Integer.parseInt(parts[1]));
            variant.setEnd(Integer.parseInt(parts[1]));
            variant.setReference(parts[2]);
            variant.setAlternate(parts[3]);
            variant.setId(parts[5]);
            variant.setStrand(strand);
            VariantType vt = VariantType.valueOf(parts[4]);
            variant.setType(vt);

            variant.setStudies(studyEntryList);

            variant.setAnnotation(null);
            variant.setHgvs(null);

            return variant;

        });

        return variantRDD1;
    }




    public JavaRDD<String> simulateVariants(JavaRDD<String[]> splitInputData) {

        JavaRDD<String> variantRDD = splitInputData.flatMap(new FlatMapFunction<String[], String>() {
            @Override
            public Iterable<String> call(String[] str) throws Exception {
                Iterable<String> var = getVariants(str);
                return var;
            }

            private Iterable<String> getVariants(String[] s) {
//                System.out.println("s.length = " + s.length);
//                System.out.println("s = " + s[0]);
//                System.out.println("s = " + s[1]);
//                System.out.println("s = " + s[2]);
//                System.out.println("s = " + s[3]);
                List<String> variants = new ArrayList<>();
                int pos = Integer.parseInt(s[1]);
                for (int i = 1; i <= Integer.parseInt(s[3]); i++) {
                    String variant = s[0] + "\t" + pos;
                    variants.add(variant);
                    pos += 1;
                }
                return variants;
            }
        });

//        JavaRDD<String> finalVar = var.map(s -> s.concat(" " + ids));
//        if (!outFilePath.isEmpty() || !outFilePath.equals("")) {
//            finalVar.saveAsTextFile(outFilePath);
//        }
//        for (String line : finalVar.collect()) {
//            System.out.println("variants = " + line);
//        }
        return variantRDD;
    }



    private JavaRDD<String> addIndelAlleles(JavaRDD<String> variantRDD) {

        JavaRDD<String> variantRDD1 = variantRDD.map((Function<String, String>) s -> {

            String[] refAltArray = new String[2];
            Random rand = new Random();
            String[] refAlt = {"A", "C", "G", "T"};

            DecimalFormat df = new DecimalFormat("#.##");
            df.setRoundingMode(RoundingMode.CEILING);
            double randomDouble = rand.nextDouble();
            String refProbabilities = df.format(randomDouble);

            String randomRef = (refAlt[rand.nextInt(refAlt.length)]);
            String randomAlt = (refAlt[rand.nextInt(refAlt.length)]);

            String[] indel = {"AC", "AT", "AG", "CGTC", "GTCAA", "CAA", "TAC", "GTCC", "CGACCCCTTTTTC", "TTCGTC"};

            String randomIndel1 = (indel[rand.nextInt(indel.length)]);
            String randomIndel2 = (indel[rand.nextInt(indel.length)]);

            if (refProbabilities.equals("0.5")) {
                refAltArray[0] = randomRef;
                refAltArray[1] = randomAlt;
            } else if (refProbabilities.equals("0.3")) {
                refAltArray[0] = randomIndel1;
                refAltArray[1] = randomIndel2;
            } else if (refProbabilities.equals("0.2")) {
                refAltArray[0] = randomIndel1;
                refAltArray[1] = randomIndel2;
            } else if (refProbabilities.equals("0.1")) {
                refAltArray[0] = randomIndel1;
                refAltArray[1] = randomIndel2;
            } else if (refProbabilities.equals("0.9")) {
                refAltArray[0] = "-";
                refAltArray[1] = randomAlt;
            } else {
                refAltArray[0] = randomRef;
                refAltArray[1] = randomAlt;
            }
            referenceAllele = refAltArray[0];
            alternateAllele = refAltArray[1];
            return s + "\t" + referenceAllele + "\t" + alternateAllele;
        });

        return variantRDD1;
    }




    private JavaRDD<String> addVariantType(JavaRDD<String> variantRDD) {

        JavaRDD<String> variantRDD2 = variantRDD.map((Function<String, String>) s -> {

            List<String> variants = new LinkedList<>();
            variants.add("SNP");
            variants.add("MNP");
            variants.add("MNV");
            variants.add("SNV");
            variants.add("INDEL");
            variants.add("SV");
            variants.add("CNV");
            variants.add("NO_VARIATION");
            variants.add("SYMBOLIC");
            variants.add("MIXED");
            String type = null;

//            System.out.println("referenceAllele.length() = " + referenceAllele.length());
//            System.out.println("alternateAllele.length() = " + alternateAllele.length());

            if (referenceAllele.length() == 1 && alternateAllele.length() > 1) {
                type = variants.get(9);
            } else if (!referenceAllele.equals("-") && referenceAllele.length() == 1) {
                if (alternateAllele.length() == 1) {
                    type = variants.get(3);
                }
            } else if (referenceAllele.length() > 1 && alternateAllele.length() > 1) {
                if (referenceAllele.length() == alternateAllele.length()) {
                    type = variants.get(2);
                } else {
                    type = variants.get(9);
                }
            } else if (referenceAllele.equals("-")) {
                type = variants.get(4);
            } else {
                type = variants.get(9);
            }
            return s + "\t" + type;
        });

        return variantRDD2;
    }



    private JavaRDD<String> addId(JavaRDD<String> variantRDD) {

        JavaRDD<String> variantRDD3 = variantRDD.map((Function<String, String>) s -> {
            Random random = new Random();
            DecimalFormat df = new DecimalFormat("#.##");
            df.setRoundingMode(RoundingMode.CEILING);
            double randomDouble = random.nextDouble();

            String idProbabilities = df.format(randomDouble);
            String id = null;
            int n = 0;
            if (idProbabilities.equals("0.1")) {
                n = 100000 + random.nextInt(900000);
                id = "rs" + Integer.toString(n);
            } else {
                id = ".";
            }
            return s + "\t" + id;
        });
        return variantRDD3;
    }










    private List<StudyEntry> getStudies(int n) {
        int studyID = 2;
        int fieldID = 3;

        List<StudyEntry> studyEntryList = new ArrayList<>();
        StudyEntry studyEntry = new StudyEntry();
        studyEntry.setStudyId(Integer.toString(studyID));
        studyEntry.setFileId(Integer.toString(fieldID));
        Map<String, String> attributes = addAttributes();
        studyEntry.setAttributes(attributes);
        studyEntry.setFormat(getFormat());
        List<List<String>> sampleList = new ArrayList<>(getFormat().size());
        for (int i = 0; i < n; i++) {
            sampleList.add(getRandomample());
        }
        studyEntry.setSamplesData(sampleList);
        studyEntryList.add(studyEntry);
        return studyEntryList;
    }



    private List<String> getRandomample() {
        Random rand = new Random();
        List<String> sample = new ArrayList<>();
        int gqValue = rand.nextInt((100) + 100);
        int dpValue = rand.nextInt((100) + 100);
        int hqValue = rand.nextInt((100) + 100);

        // Nacho example
        int genotypeIndex = rand.nextInt(1000);

        String genotype = variantSimulatorConfiguration.getGenotypeValues()[genotypeIndex];

        //sample.add("0" + "/" + "0");
        sample.add(genotype);
        sample.add(Integer.toString(gqValue));
        sample.add(Integer.toString(dpValue));
        sample.add(Integer.toString(hqValue));
        return sample;
    }



    public List<String> getFormat() {
        List<String> formatFields = new ArrayList<>(10);
        formatFields.add("GT");
        formatFields.add("GQ");
        formatFields.add("DP");
        formatFields.add("HQ");
        return formatFields;
    }


//    public static void main(String[] args) {
//        SimulatorSpark simulatorSpark = new SimulatorSpark();
//        Map<String, String> map = simulatorSpark.addAttributes();
//
//        for (Map.Entry entry : map.entrySet()) {
//            System.out.println("entry = " + entry.toString());
//        }
//    }


    private Map<String, String> addAttributes() {

//        JavaRDD<String> varRDD1 = varRDD.map((Function<String, String>) s -> {

        Random rand = new Random();
        Map<String, String> attributeMap = new HashMap<>();

//        System.out.println("alternateAllele.length() = " + alternateAllele.length());
//        System.out.println("referenceAllele.length() = " + referenceAllele.length());
        int acLength = 1; //alternateAllele.length();
        //int afLength = alternateAllele.length();
        int anLength = 1; //referenceAllele.length() + alternateAllele.length();
        //int dpLength = alternateAllele.length();

        String alleleACVal = String.valueOf(acLength);
        String alleleAFVal = String.valueOf(rand.nextInt(10));
        String alleleANVal = String.valueOf(anLength);
        String alleleDPVal = String.valueOf(rand.nextInt(200 - 100) + 100);

        attributeMap.put("AC", alleleACVal);
        attributeMap.put("AF", alleleAFVal);
        attributeMap.put("AN", alleleANVal);
        attributeMap.put("DP", alleleDPVal);

        return attributeMap;
//        });
//        return null;
    }




}
