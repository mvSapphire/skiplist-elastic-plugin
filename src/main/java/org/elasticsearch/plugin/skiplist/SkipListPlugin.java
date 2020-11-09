/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.plugin.skiplist;

import org.apache.lucene.index.LeafReaderContext;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.fielddata.ScriptDocValues;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.ScriptPlugin;
import org.elasticsearch.script.ScoreScript;
import org.elasticsearch.script.ScriptContext;
import org.elasticsearch.script.ScriptEngine;
import org.elasticsearch.search.lookup.SearchLookup;
import org.roaringbitmap.RoaringBitmap;

import java.io.DataInputStream;
import java.nio.ByteBuffer;
import java.util.Base64;
import java.util.Collection;
import java.util.Map;

public class SkipListPlugin extends Plugin implements ScriptPlugin {

    @Override
    public ScriptEngine getScriptEngine(Settings settings, Collection<ScriptContext<?>> contexts) {
        return new SkipListPluginScriptEngine();
    }

    private class SkipListPluginScriptEngine implements ScriptEngine {
        @Override
        public String getType() {
            return "skiplist";
        }

        @Override
        public <T> T compile(String scriptName, String scriptSource, ScriptContext<T> context,Map<String, String> params) {
            if (!context.equals(ScoreScript.CONTEXT)) {
                throw new IllegalArgumentException(getType() + " scripts cannot be used for context [" + context.name + "]");
            }

            if ("roaring".equals(scriptSource)) {
                ScoreScript.Factory factory = new SkipListPlugin.SkipListPluginScriptEngine.SkipListFactory();
                return context.factoryClazz.cast(factory);
            }

            throw new IllegalArgumentException("Unknown script name " + scriptSource);
        }

        private class SkipListFactory implements ScoreScript.Factory {
            @Override
            public ScoreScript.LeafFactory newFactory(Map<String, Object> params, SearchLookup lookup) {
                return new SkipListPlugin.SkipListPluginScriptEngine.SkipListLeafFactory(params, lookup);
            }
        }

        private class SkipListLeafFactory implements ScoreScript.LeafFactory {
            private final Map<String, Object> params;
            private final SearchLookup lookup;

            final String field;
            final String data;

            double skipScore = 0d;

            private SkipListLeafFactory(Map<String, Object> params, SearchLookup lookup) {
                this.params = params;
                this.lookup = lookup;

                if (!params.containsKey("field"))
                    throw new IllegalArgumentException("Missing parameter [field]");

                if (!params.containsKey("data"))
                    throw new IllegalArgumentException("Missing parameter [data]");

                field = params.get("field").toString();
                data = params.get("data").toString();
            }

            @Override
            public ScoreScript newInstance(LeafReaderContext context) {
                final RoaringBitmap bitmap_c = new RoaringBitmap();

                try {
                    bitmap_c.deserialize(new DataInputStream(new ByteBufferBackedInputStream(ByteBuffer.wrap(Base64.getDecoder().decode(data)))));
                } catch (Exception e) {
                    return new ScoreScript(params, lookup, context) {
                        @Override
                        public double execute() {
                            return get_score();
                        }
                    };
                }

                return new ScoreScript(params, lookup, context) {
                    @Override
                    public double execute() {
                        try {
                            final ScriptDocValues.Longs fieldValue = (ScriptDocValues.Longs) getDoc().get(field);
                            final int documentId = (int)fieldValue.getValue();

                            if (bitmap_c.contains(documentId))
                                return skipScore;

                        } catch (Exception e) {
                            return get_score();
                        }
                        return get_score();
                    }
                };
            }

            @Override
            public boolean needs_score() {
                return false;
            }
        }

        @Override
        public void close() {
            // optionally close resources
        }
    }
}