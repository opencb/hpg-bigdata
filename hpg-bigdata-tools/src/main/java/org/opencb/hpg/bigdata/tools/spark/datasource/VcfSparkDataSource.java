/*
 * Copyright 2015 OpenCB
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.opencb.hpg.bigdata.tools.spark.datasource;

import htsjdk.tribble.readers.LineIterator;
import htsjdk.variant.vcf.VCFHeader;
import htsjdk.variant.vcf.VCFHeaderVersion;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.opencb.biodata.formats.variant.vcf4.FullVcfCodec;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.avro.VariantAvro;
import org.opencb.biodata.tools.variant.converter.VariantContextToVariantConverter;
import org.opencb.commons.utils.FileUtils;

import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Path;

/**
 * Created by imedina on 23/01/16.
 */
public class VcfSparkDataSource extends SparkDataSource<Variant> {

    private Path filePath;


    public VcfSparkDataSource(Path path) {
        this.filePath = path;
    }

    public VcfSparkDataSource(SparkConf sparkConf, JavaSparkContext sparkContext) {
        super(sparkConf, sparkContext);

        sparkConf.registerKryoClasses(new Class[]{VariantAvro.class});
    }

    public VcfSparkDataSource(SparkConf sparkConf, JavaSparkContext sparkContext, Path filePath) {
        this(sparkConf, sparkContext);
        this.filePath = filePath;
    }

    @Override
    public JavaRDD<Variant> createRDD() throws IOException {
        FileUtils.checkFile(filePath);

        // Get a valid HTSJDK valid VCFCodec, for this VCFHeader and VCFHeaderVersion is needed.
        FullVcfCodec vcfCodec = new FullVcfCodec();
        FileInputStream fileInputStream = new FileInputStream(filePath.toFile());
        LineIterator lineIterator = vcfCodec.makeSourceFromStream(fileInputStream);
        VCFHeader vcfHeader = (VCFHeader) vcfCodec.readActualHeader(lineIterator);
        VCFHeaderVersion vcfHeaderVersion = vcfCodec.getVCFHeaderVersion();
        vcfCodec.setVCFHeader(vcfHeader, vcfHeaderVersion);
        fileInputStream.close();

        // Now we can create the RDD
        JavaRDD<String> stringJavaRDD = sparkContext.textFile(filePath.toString());
        VariantContextToVariantConverter variantContextToVariantConverter = new VariantContextToVariantConverter("", "", null);
        JavaRDD<Variant> collect = stringJavaRDD
                .filter(s -> !s.startsWith("#"))
                .map(s -> {
                    // This must be set inside the map
                    vcfCodec.setVCFHeader(vcfHeader, vcfHeaderVersion);
                    return vcfCodec.decode(s);
                })
                .map(v1 -> variantContextToVariantConverter.convert(v1));

        return collect;
    }


    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("VcfSparkDataSource{");
        sb.append("filePath=").append(filePath);
        sb.append('}');
        return sb.toString();
    }

    public Path getFilePath() {
        return filePath;
    }

    public void setFilePath(Path filePath) {
        this.filePath = filePath;
    }

}
