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

import java.io.Serializable;
import java.util.Arrays;
import java.util.Calendar;
import java.util.List;
import java.util.Map.Entry;

import org.nuxeo.ecm.core.api.NuxeoException;
import org.nuxeo.ecm.core.storage.State;

/**
 * Converts between PostgreSQL types (jsonb) and DBS types (diff, state, list, serializable).
 *
 * @since 10.10
 */
public class PGJSONConverter {

    public void valueToJson(Object value, StringBuilder buf) {
        if (value instanceof State) {
            stateToJson((State) value, buf);
        } else if (value instanceof List) {
            @SuppressWarnings("unchecked")
            List<Object> values = (List<Object>) value;
            listToJson(values, buf);
        } else if (value instanceof Object[]) {
            listToJson(Arrays.asList((Object[]) value), buf);
        } else {
            serializableToJson(value, buf);
        }
    }

    public String stateToJson(State state) {
        StringBuilder buf = new StringBuilder();
        stateToJson(state, buf);
        return buf.toString();
    }

    protected void stateToJson(State state, StringBuilder buf) {
        buf.append('{');
        boolean first = true;
        for (Entry<String, Serializable> en : state.entrySet()) {
            String key = en.getKey();
            Serializable value = en.getValue();
            if (value == null) {
                // we don't write null values
                continue;
            }
            if (first) {
                first = false;
            } else {
                buf.append(',');
            }
            buf.append('"');
            buf.append(key);
            buf.append('"');
            buf.append(':');
            valueToJson(value, buf);
        }
        buf.append('}');
    }

    public void listToJson(List<Object> values, StringBuilder buf) {
        buf.append('[');
        boolean first = true;
        for (Object value : values) {
            if (value == null) {
                // we don't write null values
                continue;
            }
            if (first) {
                first = false;
            } else {
                buf.append(',');
            }
            valueToJson(value, buf);
        }
        buf.append(']');
    }

    public String serializableToJson(Object value) {
        StringBuilder buf = new StringBuilder();
        serializableToJson(value, buf);
        return buf.toString();
    }

    public void serializableToJson(Object value, StringBuilder buf) {
        if (value instanceof String) {
            String string = (String) value;
            buf.append('"');
            if (string.indexOf('"') < 0 && string.indexOf('\\') < 0) {
                // nothing to escape, fast path
                buf.append(string);
            } else {
                buf.append(string.replace("\\", "\\\\").replace("\"", "\\\""));
            }
            buf.append('"');
        } else if (value instanceof Long || value instanceof Double || value instanceof Boolean) {
            buf.append(value);
        } else if (value instanceof Calendar) {
            long millis = ((Calendar) value).getTimeInMillis();
            buf.append(millis);
        } else {
            throw new NuxeoException("Unsupported type: " + value.getClass().getName());
        }
    }

}
