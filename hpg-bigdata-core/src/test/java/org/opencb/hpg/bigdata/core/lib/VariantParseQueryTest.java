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

package org.opencb.hpg.bigdata.core.lib;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.opencb.commons.datastore.core.Query;

import static org.junit.Assert.assertEquals;

/**
 * Created by imedina on 12/08/16.
 */
public class VariantParseQueryTest {

    private VariantParseQuery variantParseQuery;

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    public VariantParseQueryTest() {
        variantParseQuery = new VariantParseQuery();
    }

    @Test
    public void parseEmptyKey() throws Exception {
        Query query = new Query();
        query.put("", "2");

        thrown.expect(IllegalArgumentException.class);
        thrown.expectMessage("key or value are null or empty (key = , value = 2)");
        variantParseQuery.parse(query, null, "test");
    }

    @Test
    public void parseEmptyValue() throws Exception {
        Query query = new Query();
        query.put("chromosome", "");

        thrown.expect(IllegalArgumentException.class);
        thrown.expectMessage("key or value are null or empty (key = chromosome, value = )");
        variantParseQuery.parse(query, null, "test");
    }

    @Test
    public void parseConservationOR() throws Exception {
        String result = "SELECT * FROM test LATERAL VIEW explode(annotation.conservation) acons as cons " +
                "WHERE ((cons.source = 'phastCons' AND cons.score<03) " +
                "OR (cons.source = 'phylop' AND cons.score>0.5))";

        Query query = new Query();
        query.put("annotation.conservation", "phastCons<03,phylop>0.5");

        String test = variantParseQuery.parse(query, null, "test");
        assertEquals("SQL query does not match", result, test);
    }

    @Test
    public void parseConservationAND() throws Exception {
        String result = "SELECT * FROM test LATERAL VIEW explode(annotation.conservation) acons as cons " +
                "WHERE ((cons.source = 'phastCons' AND cons.score<03) " +
                "AND (cons.source = 'phylop' AND cons.score>0.5))";

        Query query = new Query();
        query.put("annotation.conservation", "phastCons<03;phylop>0.5");

        String test = variantParseQuery.parse(query, null, "test");
        assertEquals("SQL query does not match", result, test);
    }

    @Test
    public void parseConsequenceTypeName() throws Exception {
        String result = "SELECT * FROM test LATERAL VIEW explode(annotation.consequenceTypes) act as ct " +
                "LATERAL VIEW explode(ct.sequenceOntologyTerms) ctso as so WHERE so.name = 'missense_variant'";

        Query query = new Query();
        query.put("annotation.consequenceTypes.sequenceOntologyTerms.name", "missense_variant");

        String test = variantParseQuery.parse(query, null, "test");
        assertEquals("SQL query does not match", result, test);
    }

    @Test
    public void parseConsequenceTypeAccession() throws Exception {
        String result = "SELECT * FROM test LATERAL VIEW explode(annotation.consequenceTypes) act as ct " +
                "LATERAL VIEW explode(ct.sequenceOntologyTerms) ctso as so WHERE so.accession = 'SO:0001566'";

        Query query = new Query();
        query.put("annotation.consequenceTypes.sequenceOntologyTerms.accession", "SO:0001566");

        String test = variantParseQuery.parse(query, null, "test");
        assertEquals("SQL query does not match", result, test);
    }

    @Test
    public void parsePopulationFrequenciesOR() throws Exception {
        String result = "SELECT * FROM test LATERAL VIEW explode(annotation.populationFrequencies) apf as popfreq " +
                "WHERE ((popfreq.study = '1000G' AND popfreq.population = 'CEU' AND popfreq.altAlleleFreq<1.2) " +
                "OR (popfreq.study = '1000G' AND popfreq.population = 'ASW' AND popfreq.altAlleleFreq<1.25))";

        Query query = new Query();
        query.put("annotation.populationFrequencies.altAlleleFreq", "1000G::CEU<1.2,1000G::ASW<1.25");

        String test = variantParseQuery.parse(query, null, "test");
        assertEquals("SQL query does not match", result, test);
    }

    @Test
    public void parsePopulationFrequenciesAND() throws Exception {
        String result = "SELECT * FROM test LATERAL VIEW explode(annotation.populationFrequencies) apf as popfreq " +
                "WHERE ((popfreq.study = '1000G' AND popfreq.population = 'CEU' AND popfreq.altAlleleFreq<1.2) " +
                "AND (popfreq.study = '1000G' AND popfreq.population = 'ASW' AND popfreq.altAlleleFreq<1.25))";

        Query query = new Query();
        query.put("annotation.populationFrequencies.altAlleleFreq", "1000G::CEU<1.2;1000G::ASW<1.25");

        String test = variantParseQuery.parse(query, null, "test");
        assertEquals("SQL query does not match", result, test);
    }

    @Test
    public void parseMultipleIds() throws Exception {
        String result = "SELECT * FROM test  WHERE (id = 'rs587604674' OR id = 'rs587603352')";

        Query query = new Query();
        query.put("id", "rs587604674,rs587603352");

        String test = variantParseQuery.parse(query, null, "test");
        assertEquals("SQL query does not match", result, test);
    }

}