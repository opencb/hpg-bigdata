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

package org.opencb.hpg.bigdata.core.converters.variation;

import htsjdk.variant.variantcontext.VariantContext;
import htsjdk.variant.vcf.VCFHeader;
import htsjdk.variant.vcf.VCFHeaderVersion;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import org.opencb.biodata.models.variant.avro.Variant;
import org.opencb.biodata.models.variant.converter.VariantContextToVariantConverter;
import org.opencb.commons.run.ParallelTaskRunner;
import org.opencb.hpg.bigdata.core.converters.FullVcfCodec;
import org.opencb.hpg.bigdata.core.io.avro.AvroEncoder;
import org.opencb.hpg.bigdata.core.utils.VariantContextBlockIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by hpccoll1 on 10/04/15.
 */
public class VariantAvroEncoderTask implements ParallelTaskRunner.Task<CharBuffer, ByteBuffer> {

    protected Logger logger = LoggerFactory.getLogger(this.getClass().toString());

//    private final VariantConverterContext variantConverterContext;

    private final VCFHeader header;
    private final AvroEncoder<Variant> encoder;
    private final VariantContextToVariantConverter converter;
    private final VariantContextBlockIterator variantContextBlockIterator;
    private final FullVcfCodec codec;

    private static AtomicLong parseTime = new AtomicLong(0);
    private static AtomicLong convertTime = new AtomicLong(0);
    private static AtomicLong encodeTime = new AtomicLong(0);
    private static AtomicBoolean postDone = new AtomicBoolean(false);

    private int failConvert = 0;

    public VariantAvroEncoderTask(VCFHeader header, VCFHeaderVersion version) {
        this.header = header;
        codec = new FullVcfCodec();
        codec.setVCFHeader(this.header, version);
        converter = new VariantContextToVariantConverter();
        encoder = new AvroEncoder<>(Variant.getClassSchema());
        variantContextBlockIterator = new VariantContextBlockIterator(codec);
        variantContextBlockIterator.setDecodeGenotypes(false);
    }


    @Override
    public void pre() {
        int gtSize = header.getGenotypeSamples().size();
        List<String> genotypeSamples = header.getGenotypeSamples();
    }

    @Override
    public List<ByteBuffer> apply(List<CharBuffer> charBufferList) {
        List<Variant> convertedList = new ArrayList<>(charBufferList.size());
        List<ByteBuffer> encoded;

        //Parse from CharBuffer to VariantContext
        long start = System.nanoTime();
        List<VariantContext> variantContexts = variantContextBlockIterator.convert(charBufferList);
        parseTime.addAndGet(System.nanoTime() - start);

        // Convert to Variants
        start = System.nanoTime();
        for (VariantContext variantContext : variantContexts) {
            try {
                convertedList.add(converter.convert(variantContext));
            } catch (Exception e) {
                e.printStackTrace();
                failConvert++;
            }
        }
        convertTime.addAndGet(System.nanoTime() - start);
        logger.debug("[" + Thread.currentThread().getName() + "] Processed " + variantContexts.size()
                + " variants into " + convertedList.size() + " avro variants");

        //Encode with Avro
        try {
            start = System.nanoTime();
            encoded = encoder.encode(convertedList);
            encodeTime.addAndGet(System.nanoTime() - start);
            logger.debug("[" + Thread.currentThread().getName() + "] Processed " + convertedList.size()
                    + " avro variants into " + encoded.size() + " encoded variants");
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
