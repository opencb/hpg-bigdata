package org.opencb.hpg.bigdata.core.converters.variation;

import htsjdk.variant.variantcontext.VariantContext;
import htsjdk.variant.vcf.VCFHeader;
import htsjdk.variant.vcf.VCFHeaderVersion;
import org.ga4gh.models.CallSet;
import org.ga4gh.models.Variant;
import org.ga4gh.models.VariantSet;
import org.opencb.commons.run.ParallelTaskRunner;
import org.opencb.hpg.bigdata.core.converters.FullVCFCodec;
import org.opencb.hpg.bigdata.core.io.avro.AvroEncoder;
import org.opencb.hpg.bigdata.core.utils.VariantContextBlockIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by hpccoll1 on 10/04/15.
 */
public class VariantAvroEncoderTask implements ParallelTaskRunner.Task<CharBuffer, ByteBuffer> {
    protected Logger logger = LoggerFactory.getLogger(this.getClass().toString());

    private final VariantConverterContext variantConverterContext;

    private final VCFHeader header;
    private final AvroEncoder<Variant> encoder;
    private final VariantContext2VariantConverter converter;
    private final VariantContextBlockIterator variantContextBlockIterator;
    private final FullVCFCodec codec;

    static AtomicLong parseTime = new AtomicLong(0);
    static AtomicLong convertTime = new AtomicLong(0);
    static AtomicLong encodeTime = new AtomicLong(0);
    static AtomicBoolean postDone = new AtomicBoolean(false);

    int failConvert = 0;

    public VariantAvroEncoderTask(VariantConverterContext variantConverterContext, VCFHeader header, VCFHeaderVersion version) {
        this.variantConverterContext = variantConverterContext;
        this.header = header;
        codec = new FullVCFCodec();
        codec.setVCFHeader(this.header, version);
        converter = new VariantContext2VariantConverter();
        encoder = new AvroEncoder<>(Variant.getClassSchema());
        variantContextBlockIterator = new VariantContextBlockIterator(codec);
    }


    @Override
    public void pre() {
        int gtSize = header.getGenotypeSamples().size();


        VariantSet vs = new VariantSet();
//        vs.setId(file.getName());
//        vs.setDatasetId(file.getName());
//        vs.setReferenceSetId("test");
        vs.setId("test"); //TODO
        vs.setDatasetId("test");
        vs.setReferenceSetId("test");

        List<String> genotypeSamples = header.getGenotypeSamples();
        Genotype2CallSet gtConverter = new Genotype2CallSet();
        for(int gtPos = 0; gtPos < gtSize; ++gtPos){
            CallSet cs = gtConverter.forward(genotypeSamples.get(gtPos));
            cs.getVariantSetIds().add(vs.getId());
            variantConverterContext.getCallSetMap().put(cs.getName(), cs);
//                callWriter.write(cs);
        }

        converter.setContext(variantConverterContext);
    }

    @Override
    public List<ByteBuffer> apply(List<CharBuffer> charBufferList) {
        List<Variant> convertedList = new ArrayList<>(charBufferList.size());
        List<ByteBuffer> encoded;

        long start;

        //Parse from CharBuffer to VariantContext
        start = System.nanoTime();
        List<VariantContext> variantContexts = variantContextBlockIterator.convert(charBufferList);
        parseTime.addAndGet(System.nanoTime() - start);


        // Convert to GA4GH Variants
        start = System.nanoTime();
        for (VariantContext variantContext : variantContexts) {
            try {
                convertedList.add(converter.forward(variantContext));
            } catch (Exception e) {
                e.printStackTrace();
                failConvert++;
            }
        }
        convertTime.addAndGet(System.nanoTime() - start);
        logger.debug("[" + Thread.currentThread().getName() + "] Processed " + variantContexts.size() + " variants into " + convertedList.size() + " avro variants");

        //Encode with Avro
        try {
            start = System.nanoTime();
            encoded = encoder.encode(convertedList);
            encodeTime.addAndGet(System.nanoTime() - start);
            logger.debug("[" + Thread.currentThread().getName() + "] Processed " + convertedList.size() + " avro variants into " + encoded.size() + " encoded variants");
            return encoded;
        } catch (IOException e) {
            e.printStackTrace();
            return Collections.emptyList();
        }
    }

    @Override
    public void post() {
        if (!postDone.getAndSet(true)) {
            logger.debug("parseTime = " + parseTime.get() / 1000000000.0 + "s");
            logger.debug("convertTime = " + convertTime.get() / 1000000000.0 + "s");
            logger.debug("encodeTime = " + encodeTime.get() / 1000000000.0 + "s");
        }
    }

}