/*
 * (C) Copyright 2018 Nuxeo (http://nuxeo.com/) and others.
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
 *
 * Contributors:
 *     Florent Guillaume
 */
package org.nuxeo.ecm.core.storage.pgjson;

import static org.junit.Assert.assertEquals;

import java.io.Serializable;
import java.time.ZonedDateTime;
import java.util.Arrays;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.List;

import org.junit.Test;
import org.nuxeo.ecm.core.storage.State;

public class TestPGJSONConverter {

    protected PGJSONConverter converter = new PGJSONConverter();

    protected String stateToJson(State state) {
        return converter.stateToJson(state);
    }

    protected String listToJson(List<Object> list) {
        StringBuilder buf = new StringBuilder();
        converter.listToJson(list, buf);
        return buf.toString();
    }

    protected String serializableToJson(Object value) {
        return converter.serializableToJson(value);
    }

    @Test
    public void testString() {
        assertEquals("\"\"", serializableToJson(""));
        assertEquals("\"\\\\\"", serializableToJson("\\"));
        assertEquals("\"\\\"\"", serializableToJson("\""));
        assertEquals("\"AB\"", serializableToJson("AB"));
        assertEquals("\"A\\\\B\"", serializableToJson("A\\B"));
        assertEquals("\"A\\\"B\"", serializableToJson("A\"B"));
    }

    @Test
    public void testLong() {
        assertEquals("0", serializableToJson(Long.valueOf(0)));
        assertEquals("123456789", serializableToJson(Long.valueOf(123456789)));
        assertEquals("-123", serializableToJson(Long.valueOf(-123)));
    }

    @Test
    public void testDouble() {
        assertEquals("0.0", serializableToJson(Double.valueOf(0)));
        assertEquals("1.234", serializableToJson(Double.valueOf(1.234)));
    }

    @Test
    public void testBoolean() {
        assertEquals("false", serializableToJson(Boolean.FALSE));
        assertEquals("true", serializableToJson(Boolean.TRUE));
    }

    @Test
    public void testCalendar() {
        Calendar cal = GregorianCalendar.from(ZonedDateTime.parse("2019-01-02T15:45:42.123Z"));
        assertEquals("1546443942123", serializableToJson(cal));
    }

    @Test
    public void testList() {
        assertEquals("[]", listToJson(Arrays.asList()));
        assertEquals("[\"foo\"]", listToJson(Arrays.asList("foo")));
        assertEquals("[\"foo\",\"bar\"]", listToJson(Arrays.asList("foo", "bar")));
        assertEquals("[\"foo\",123]", listToJson(Arrays.asList("foo", Long.valueOf(123))));
        // nulls are skipped
        assertEquals("[\"foo\",\"bar\"]", listToJson(Arrays.asList("foo", null, "bar")));
    }

    @Test
    public void testState() {
        State state = new State();
        assertEquals("{}", stateToJson(state));
        state.put("foo", "bar");
        assertEquals("{\"foo\":\"bar\"}", stateToJson(state));
        state.put("gee", "moo");
        // State keeps order for a small number of keys so this is ok
        assertEquals("{\"foo\":\"bar\",\"gee\":\"moo\"}", stateToJson(state));
        state.put("foo", null);
        assertEquals("{\"gee\":\"moo\"}", stateToJson(state));

        // nested
        State state2 = new State();
        state2.put("a", Long.valueOf(123));
        state2.put("b", (Serializable) Arrays.asList((Serializable) Boolean.TRUE, Double.valueOf(3.14)));
        state.put("bar", state2);
        assertEquals("{\"gee\":\"moo\",\"bar\":{\"a\":123,\"b\":[true,3.14]}}", stateToJson(state));
    }

}
