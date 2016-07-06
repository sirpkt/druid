/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.segment.serde;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import io.druid.segment.GenericColumnSerializer;
import io.druid.segment.column.ColumnBuilder;
import io.druid.segment.column.ColumnConfig;
import io.druid.segment.column.ComplexColumn;
import io.druid.segment.column.ValueType;
import io.druid.segment.data.CompressedFixedSizeComplexesIndexedSupplier;
import io.druid.segment.data.GenericIndexed;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.WritableByteChannel;

/**
 */
public class ComplexColumnPartSerde implements ColumnPartSerde
{
  @JsonCreator
  public static ComplexColumnPartSerde createDeserializer(
      @JsonProperty("typeName") String complexType,
      @JsonProperty("byteOrder") ByteOrder byteOrder
      )
  {
    return new ComplexColumnPartSerde(complexType, byteOrder, null);
  }

  private final String typeName;
  private final ComplexMetricSerde serde;
  private final ByteOrder byteOrder;
  private final Serializer serializer;

  private ComplexColumnPartSerde(String typeName, ByteOrder byteOrder, Serializer serializer)
  {
    this.typeName = typeName;
    this.serde = ComplexMetrics.getSerdeForType(typeName);
    this.byteOrder = byteOrder;
    this.serializer = serializer;
  }

  @JsonProperty
  public String getTypeName()
  {
    return typeName;
  }

  @JsonProperty
  public ByteOrder getByteOrder()
  {
    return byteOrder;
  }

  public static SerializerBuilder serializerBuilder()
  {
    return new SerializerBuilder();
  }

  public static class SerializerBuilder
  {
    private String typeName = null;
    private ByteOrder byteOrder = null;
    private GenericColumnSerializer delegate = null;

    public SerializerBuilder withTypeName(final String typeName)
    {
      this.typeName = typeName;
      return this;
    }

    public SerializerBuilder withByteOrder(final ByteOrder byteOrder)
    {
      this.byteOrder = byteOrder;
      return this;
    }

    public SerializerBuilder withDelegate(final GenericColumnSerializer delegate)
    {
      this.delegate = delegate;
      return this;
    }

    public ComplexColumnPartSerde build()
    {
      return new ComplexColumnPartSerde(
          typeName, byteOrder, new Serializer()
      {
        @Override
        public long numBytes()
        {
          return delegate.getSerializedSize();
        }

        @Override
        public void write(WritableByteChannel channel) throws IOException
        {
          delegate.writeToChannel(channel);
        }
      }
      );
    }
  }

  public static LegacySerializerBuilder legacySerializerBuilder()
  {
    return new LegacySerializerBuilder();
  }

  public static class LegacySerializerBuilder
  {
    private String typeName = null;
    private ByteOrder byteOrder = null;
    private Object delegate = null;

    public LegacySerializerBuilder withTypeName(final String typeName)
    {
      this.typeName = typeName;
      return this;
    }

    public LegacySerializerBuilder withByteOrder(final ByteOrder byteOrder)
    {
      this.byteOrder = byteOrder;
      return this;
    }

    public LegacySerializerBuilder withDelegate(final Object delegate)
    {
      Preconditions.checkArgument(delegate instanceof GenericIndexed || delegate instanceof CompressedFixedSizeComplexesIndexedSupplier,
          "delegate should be GenericIndexed or CompressedFixedSizeComplexesIndexedSupplier");
      this.delegate = delegate;
      return this;
    }

    public ComplexColumnPartSerde build()
    {
      final GenericIndexed genericIndexed;
      final CompressedFixedSizeComplexesIndexedSupplier compressedFixedSizeComplexesIndexedSupplier;

      if (delegate instanceof GenericIndexed) {
        genericIndexed = (GenericIndexed)delegate;
        compressedFixedSizeComplexesIndexedSupplier = null;
      } else {
        genericIndexed = null;
        compressedFixedSizeComplexesIndexedSupplier = (CompressedFixedSizeComplexesIndexedSupplier)delegate;
      }

      return new ComplexColumnPartSerde(
          typeName, byteOrder, new Serializer()
      {
        @Override
        public long numBytes()
        {
          if (genericIndexed != null)
            return genericIndexed.getSerializedSize();
          else
            return compressedFixedSizeComplexesIndexedSupplier.getSerializedSize();
        }

        @Override
        public void write(WritableByteChannel channel) throws IOException
        {
          if (genericIndexed != null)
            genericIndexed.writeToChannel(channel);
          else
            compressedFixedSizeComplexesIndexedSupplier.writeToChannel(channel);
        }
      }
      );
    }
  }

  @Override
  public Serializer getSerializer()
  {
    return serializer;
  }

  @Override
  public Deserializer getDeserializer()
  {
    return new Deserializer()
    {
      @Override
      public void read(ByteBuffer buffer, ColumnBuilder builder, ColumnConfig columnConfig)
      {
        if (serde != null) {
          if (serde.getMetricSize() != null) {
            final CompressedFixedSizeComplexesIndexedSupplier column = CompressedFixedSizeComplexesIndexedSupplier.fromByteBuffer(
                buffer,
                byteOrder,
                serde
            );
            builder.setType(ValueType.COMPLEX)
                   .setHasMultipleValues(false)
                   .setComplexColumn(
                       new Supplier<ComplexColumn>() {
                         @Override
                         public ComplexColumn get() {
                           return column.get();
                         }
                       }
                   );
          } else {
            serde.deserializeColumn(buffer, builder);
          }
        }
      }
    };
  }
}
