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
    public void parse() throws Exception {
        String result = "SELECT * FROM test LATERAL VIEW explode(annotation.conservation) acons as cons_phylop " +
                "LATERAL VIEW explode(annotation.conservation) acons as cons_phastCons " +
                "WHERE ((cons_phastCons.source = 'phastCons' AND cons_phastCons.score<03) " +
                "OR (cons_phylop.source = 'phylop' AND cons_phylop.score>0.5))";

        Query query = new Query();
        query.put("annotation.conservation", "phastCons<03,phylop>0.5");

        String test = variantParseQuery.parse(query, null, "test");
        assertEquals("SQL query does not match", result, test);
    }

    @Test
    public void parseEmptyKeyValue() throws Exception {
        Query query = new Query();
        query.put("chromosome", "");

        thrown.expect(IllegalArgumentException.class);
        thrown.expectMessage("key or value are null or empty");
        variantParseQuery.parse(query, null, "test");
    }

}