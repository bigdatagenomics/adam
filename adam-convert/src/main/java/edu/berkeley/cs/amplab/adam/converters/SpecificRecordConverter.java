/*
 * Copyright (c) 2013. Regents of the University of California
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

package edu.berkeley.cs.amplab.adam.converters;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.avro.specific.SpecificRecordBase;

/**
 * This class converters a flat specific record to a GenericRecord. Records with
 * nested complex types (e.g. arrays, records) are not supported
 *
 * @param <T>
 */
public class SpecificRecordConverter<T extends SpecificRecordBase> implements Function<T, GenericRecord> {

    @Override
    public GenericRecord apply(T record) {
        Preconditions.checkNotNull(record, "Unable to convert a null record");
        Schema schema = record.getSchema();
        GenericRecordBuilder builder = new GenericRecordBuilder(schema);
        for (Schema.Field field : schema.getFields()) {
            builder.set(field, record.get(field.pos()));
        }
        return builder.build();
    }
}
