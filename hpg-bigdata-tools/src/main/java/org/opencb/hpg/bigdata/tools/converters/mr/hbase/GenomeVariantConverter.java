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

package org.opencb.hpg.bigdata.tools.converters.mr.hbase;

import org.apache.avro.io.DatumWriter;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.ga4gh.models.Call;
import org.ga4gh.models.Variant;
import org.opencb.hpg.bigdata.tools.utils.HBaseUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.Map.Entry;
import java.util.stream.Collectors;

/**
 * @author Matthias Haimel mh719+git@cam.ac.uk
 */
public class GenomeVariantConverter extends Mapper<AvroKey<Variant>, NullWritable, String, ByteBuffer> {
    private final Logger log = LoggerFactory.getLogger(GenomeVariantConverter.class);
    private static final String ILLUMINA_GVCF_BLOCK_END = "END";

    public static final Integer BUCKET_SIZE = 100;

    private DatumWriter<Variant> variantDatumWriter = new SpecificDatumWriter<Variant>(Variant.class);


    public GenomeVariantConverter() {
        // TODO Auto-generated constructor stub
    }

    @Override
    protected void map(AvroKey<Variant> key, NullWritable value, Mapper<AvroKey<Variant>, NullWritable, String, ByteBuffer>.Context context)
            throws IOException, InterruptedException {
        Variant variant = key.datum();
        List<Variant> varList = process(context, variant);
        Map<String, List<Variant>> grouped = groupVariants(varList);
        Map<String, ByteBuffer> packageVariants = packageVariants(grouped);
        submit(context, packageVariants);
    }

    private void submit(Context context, Map<String, ByteBuffer> packageVariants) throws IOException, InterruptedException {
        for (Entry<String, ByteBuffer> entry : packageVariants.entrySet()) {
            context.write(entry.getKey(), entry.getValue());
        }
    }

    private ByteBuffer convert(List<Variant> list) {
        StringBuilder sb = new StringBuilder();
        for (Variant variant : list) {
            sb.append(variant); // as JSON for testing TODO change to propper format
        }
        ByteBuffer wrap = ByteBuffer.wrap(sb.toString().getBytes());
        return wrap;
    }

    private Map<String, ByteBuffer> packageVariants(Map<String, List<Variant>> groups) {
        Map<String, ByteBuffer> pack = new HashMap<>();
        groups.forEach((k, v) -> pack.put(k, convert(v)));
        return pack;
    }

    private String generateBlockId(Variant var) {
        long start = var.getStart().longValue() / BUCKET_SIZE;
        StringBuilder sb = new StringBuilder(var.getReferenceName());
        sb.append("_");
        sb.append(start);
        return sb.toString();
    }

    private Map<String, List<Variant>> groupVariants(List<Variant> varList) {
        Map<String, List<Variant>> grouped = varList.stream().collect(Collectors.groupingBy(var -> generateBlockId(var)));
        return grouped;
    }

    public List<Variant> process(Context context, Variant variant) {
        if (isReference(variant)) { // is just reference
            List<Variant> varList = expandReferenceRegion(context, variant);
            return varList;
        } else { // is a variant (not just coverage info)
            List<Variant> varList = expandAltRegion(context, variant);
            return varList;

            /* Ignore fields */
//          List<String> ids = v.getAlleleIds(); // graph mode -> not supported
        }
    }

    private List<Variant> expandAltRegion(Context context, Variant variant) {
        List<Variant> varList = new ArrayList<Variant>(1);
        String refBases = variant.getReferenceBases();
        List<String> altBasesList = variant.getAlternateBases();

        int altCnt = altBasesList.size();
        List<Call> calls = nonull(variant.getCalls());

        if (altCnt > 1) {
            context.getCounter("VCF", "biallelic_COUNT").increment(1);
            // e.g.
            // 1       10409   .       ACCCTAACCCTAACCCTAACCCTAACCCTAAC        A,ACCCTAACCCTAACCCTAACCCTAACCCTAA
            // GT:GQ:GQX:DPI:AD  1/2:265:12:14:4,6,6
        }
        if (calls.isEmpty()) { // IF not call made - still store it.
            context.getCounter("VCF", "NO_CALL_COUNT").increment(1);
        }

        for (int altIdx = 0; altIdx < altCnt; ++altIdx) {
            String altBases = altBasesList.get(altIdx);
//            variant.getAlleleIds() TODO
            if (altBases.length() >= HBaseUtils.SV_THRESHOLD || refBases.length() >= HBaseUtils.SV_THRESHOLD) {
                // KEEP SV information for the moment - evaluate if there are issues.
                // TODO change if needed
                context.getCounter("VCF", "SV_LIMIT_REACHED_COUNT").increment(1);
            }
        }
        varList.add(variant);
        return varList;
    }

    private boolean isReference(Variant variant) {
        return null == variant.getAlternateBases() || variant.getAlternateBases().isEmpty();
    }

    protected List<Variant> expandReferenceRegion(Context context, Variant variant) {
        Long start = variant.getStart();
        Long endPos = start + 1;
        List<Call> calls = nonull(variant.getCalls());
        boolean nocall = calls.isEmpty();
        context.getCounter("VCF", "REG_EXPAND" + (nocall ? "_NOCALL" : "")).increment(1);

        Map<String, List<String>> info = new HashMap<String, List<String>>(variant.getInfo());

        List<String> endLst = nonull(info.remove(ILLUMINA_GVCF_BLOCK_END)); // Get End position

        if (endLst.isEmpty()) {
            // Region of size 1
            context.getCounter("VCF", "REF_END_EMPTY" + (nocall ? "_NOCALL" : "")).increment(1);
        } else {
            String endStr = endLst.get(0).toString();
            endPos = Long.valueOf(endStr);
        }
        String counterName = "REG_EXPAND_CNT" + (nocall ? "_NOCALL" : "");
        context.getCounter("VCF", counterName).increment((endPos - start));

        List<Variant> expVarList = expand(variant, start, endPos, info, calls);

        return expVarList;
    }

    protected List<Variant> expand(Variant variant, Long start, Long end, Map<String, List<String>> info, List<Call> calls) {
        List<Variant> varList = new ArrayList<>(Long.valueOf((end - start)).intValue());

        List<String> names = variant.getNames();
        String setId = variant.getVariantSetId();
        Long created = variant.getCreated();
        String id = variant.getId();
        Long updated = variant.getUpdated();
        String refName = variant.getReferenceName();
        String refBases = variant.getReferenceBases();

        // the following parameters shouldn't really matter
//        List<String> alleleIds = variant.getAlleleIds();
        List<String> alternateBases = variant.getAlternateBases();

        for (Long pos = start; pos < end; ++pos) {
            Variant var = new Variant();

            /* from parameter */
            var.setStart(pos);
            var.setEnd(pos + 1);
            var.setInfo(info);
            var.setCalls(calls);

            /* from variant */
            var.setNames(names);
            var.setVariantSetId(setId);
            var.setCreated(created);
            var.setId(id);
            var.setUpdated(updated);
            var.setReferenceName(refName);
            var.setReferenceBases(refBases);
            var.setAlternateBases(alternateBases);
//            var.setAlleleIds(alleleIds);

            /* ADD */
            varList.add(var);
        }
        return varList;
    }


    private <T> List<T> nonull(List<T> list) {
        if (null == list) {
            return Collections.emptyList();
        }
        return list;
    }

//    public static Logger getLog() {
//        return log;
//    }

}
