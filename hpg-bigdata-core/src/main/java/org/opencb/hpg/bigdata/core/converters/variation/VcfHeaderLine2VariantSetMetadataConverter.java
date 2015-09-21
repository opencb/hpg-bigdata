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

/**
 *
 */
package org.opencb.hpg.bigdata.core.converters.variation;

import htsjdk.variant.vcf.VCFCompoundHeaderLine;
import htsjdk.variant.vcf.VCFCompoundHeaderLine.SupportedHeaderLineType;
import htsjdk.variant.vcf.VCFFilterHeaderLine;
import htsjdk.variant.vcf.VCFFormatHeaderLine;
import htsjdk.variant.vcf.VCFHeaderLine;
import htsjdk.variant.vcf.VCFHeaderLineCount;
import htsjdk.variant.vcf.VCFHeaderLineType;
import htsjdk.variant.vcf.VCFInfoHeaderLine;
import htsjdk.variant.vcf.VCFSimpleHeaderLine;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.ga4gh.models.VariantSetMetadata;
import org.opencb.hpg.bigdata.core.converters.Converter;

/**
 * @author mh719
 *
 */
public class VcfHeaderLine2VariantSetMetadataConverter implements Converter<VCFHeaderLine, VariantSetMetadata> {

    @Override
    public VariantSetMetadata forward(VCFHeaderLine hl) {
        if (null == hl) {
            return null;
        }
        if (hl instanceof VCFInfoHeaderLine) {
            return convert((VCFInfoHeaderLine) hl);
        }
        if (hl instanceof VCFFormatHeaderLine) {
            return convert((VCFFormatHeaderLine) hl);
        }
//      if(hl instanceof VCFContigHeaderLine )
//          return convert((VCFContigHeaderLine) hl);
        if (hl instanceof VCFFilterHeaderLine) {
            return convert((VCFFilterHeaderLine) hl);
        }
//      if(VCFHeaderLine.class.equals(hl.getClass())){
//          return convert(hl);
//      }
//      if(VCFSimpleHeaderLine.class.equals(hl.getClass())){
//          return convert((VCFSimpleHeaderLine) hl);
//      }
        throw new UnsupportedOperationException(String.format("Header line not supported yet: %s", hl.getClass()));
    }

    private VariantSetMetadata convert(VCFHeaderLine hl) {
        VariantSetMetadata vsm = new VariantSetMetadata();
        vsm.setKey(hl.getKey());
        vsm.setValue(hl.getValue());
        return vsm;
    }

    private VariantSetMetadata convert(VCFSimpleHeaderLine hl) {
        VariantSetMetadata vsm = convert((VCFHeaderLine) hl);
        vsm.setId(hl.getID());
        return vsm;
    }

    private VariantSetMetadata convert(VCFFilterHeaderLine hl) {
        return convert((VCFSimpleHeaderLine) hl);
    }

    private VariantSetMetadata convert(VCFCompoundHeaderLine hl) {
        VariantSetMetadata vsm = convert((VCFHeaderLine) hl);
        vsm.setId(hl.getID());
        if (hl.isFixedCount()) {
            vsm.setNumber(Integer.toString(hl.getCount()));
        } else {
            vsm.setNumber(hl.getCountType().name());
        }
        vsm.setType(hl.getType().name());
        vsm.setDescription(hl.getDescription());
        // Empty for the moment
        Map<CharSequence, List<CharSequence>> infoMap = Collections.emptyMap();
        vsm.setInfo(infoMap);
        return vsm;
    }

    private VariantSetMetadata convert(VCFFormatHeaderLine hl) {
        return convert((VCFCompoundHeaderLine) hl);
    }
    private VariantSetMetadata convert(VCFInfoHeaderLine hl) {
        return convert((VCFCompoundHeaderLine) hl);
    }



    private VCFInfoHeaderLine convertToInfo(VariantSetMetadata vsm) {
        String nbr = vsm.getNumber().toString();
        VCFHeaderLineCount cl = valueOfLineCountHeader(nbr);
        VCFInfoHeaderLine hl;
        if (null != cl) {
            hl = new VCFInfoHeaderLine(vsm.getId().toString(), cl,
                    VCFHeaderLineType.valueOf(vsm.getType().toString()), vsm.getDescription().toString());
        } else {
            hl = new VCFInfoHeaderLine(vsm.getId().toString(), Integer.valueOf(vsm.getNumber().toString()),
                    VCFHeaderLineType.valueOf(vsm.getType().toString()), vsm.getDescription().toString());
        }
        return hl;
    }

    private VCFHeaderLineCount valueOfLineCountHeader(String nbr) {
        for (VCFHeaderLineCount lc : VCFHeaderLineCount.values()) {
            if (StringUtils.equals(lc.name(), nbr)) {
                return lc;
            }
        }
        return null;
    }

    private VCFHeaderLine convertToFormat(VariantSetMetadata vsm) {
        String nbr = vsm.getNumber().toString();
        VCFHeaderLineCount cl = valueOfLineCountHeader(nbr);
        VCFFormatHeaderLine hl;
        if (null != cl) {
            hl = new VCFFormatHeaderLine(vsm.getId().toString(), cl,
                    VCFHeaderLineType.valueOf(vsm.getType().toString()), vsm.getDescription().toString());
        } else {
            hl = new VCFFormatHeaderLine(vsm.getId().toString(), Integer.valueOf(nbr),
                    VCFHeaderLineType.valueOf(vsm.getType().toString()), vsm.getDescription().toString());
        }

        return hl;
    }

    @Override
    public VCFHeaderLine backward(VariantSetMetadata vsm) {
        if (null == vsm) {
            return null;
        } else if (StringUtils.equals(vsm.getKey().toString(), SupportedHeaderLineType.INFO.name())) {
            return convertToInfo(vsm);
        } else if (StringUtils.equals(vsm.getKey().toString(), SupportedHeaderLineType.FORMAT.name())) {
            return convertToFormat(vsm);
        } else if (StringUtils.equals("contig", vsm.getKey().toString())) {
            throw new UnsupportedOperationException(
                    String.format("Header line not supported yet: %s", vsm.getKey().toString()));
        } else if (StringUtils.equals("fileformat", vsm.getKey().toString())) {
            return convertTo(vsm.getKey(), vsm.getValue());
        }
        throw new UnsupportedOperationException(
                String.format("Header line currently not supported: %s", vsm.getKey().toString()));
    }

    private VCFHeaderLine convertTo(CharSequence key, CharSequence value) {
        return new VCFHeaderLine(key.toString(), value.toString());
    }

}
