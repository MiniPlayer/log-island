/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.hurence.logisland.serializer;

import com.hurence.logisland.logging.ComponentLog;
import com.hurence.logisland.logging.LogLevel;
import com.hurence.logisland.logging.StandardComponentLogger;
import org.apache.avro.Schema;

public class SerializerProvider {


    private static ComponentLog logger = new StandardComponentLogger("serializerProvider", SerializerProvider.class);
    private static String AVRO_SERIALIZER = AvroSerializer.class.getName();
    private static String JSON_SERIALIZER = JsonSerializer.class.getName();
    private static String KRYO_SERIALIZER = KryoSerializer.class.getName();
    private static String BYTES_ARRAY_SERIALIZER = BytesArraySerializer.class.getName();
    private static String NOOP_SERIALIZER = NoopSerializer.class.getName();

    /**
     * build a serializer
     *
     * @param inSerializerClass the serializer type
     * @param schemaContent     an optional Avro schema
     * @return the serializer
     */
    public static RecordSerializer getSerializer(final String inSerializerClass, final String schemaContent) {

        try {
            if (inSerializerClass.equals(AVRO_SERIALIZER)) {
                Schema.Parser parser = new Schema.Parser();
                Schema schema = parser.parse(schemaContent);
                return new AvroSerializer(schema);
            } else if (inSerializerClass.equals(JSON_SERIALIZER)) {
                return new JsonSerializer();
            } else if (inSerializerClass.equals(KRYO_SERIALIZER)) {
                return new KryoSerializer(true);
            } else if (inSerializerClass.equals(BYTES_ARRAY_SERIALIZER)) {
                return new BytesArraySerializer();
            } else {
                return new NoopSerializer();
            }
        } catch (Exception e) {
            logger.log(LogLevel.DEBUG, e.toString());
            return new NoopSerializer();
        }

    }
}
