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

import com.google.common.base.Preconditions;
import com.google.common.io.Files;
import io.druid.segment.GenericColumnSerializer;
import io.druid.segment.IndexIO;
import io.druid.segment.MetricColumnSerializer;
import io.druid.segment.MetricHolder;
import io.druid.segment.data.CompressedFixedSizeObjectSupplierSerializer;
import io.druid.segment.data.CompressedObjectStrategy;
import io.druid.segment.data.IOPeon;

import java.io.File;
import java.io.IOException;
import java.nio.ByteOrder;
import java.nio.channels.WritableByteChannel;

public class FixedSizeComplexColumnSerializer implements GenericColumnSerializer
{
  private final IOPeon ioPeon;
  private final String filenameBase;
  private final ByteOrder byteOrder;
  private final CompressedObjectStrategy.CompressionStrategy compression;
  private final ComplexMetricSerde serde;

  private CompressedFixedSizeObjectSupplierSerializer writer;

  public FixedSizeComplexColumnSerializer(
    IOPeon ioPeon,
    String filenameBase,
    ByteOrder byteOrder,
    CompressedObjectStrategy.CompressionStrategy compression,
    ComplexMetricSerde serde
  )
  {
    Preconditions.checkNotNull(serde.getMetricSize(), "Should be fixed size");

    this.ioPeon = ioPeon;
    this.filenameBase = filenameBase;
    this.byteOrder = byteOrder;
    this.compression = compression;
    this.serde = serde;
  }

  @Override
  public void open() throws IOException
  {
    writer = CompressedFixedSizeObjectSupplierSerializer.create(
        ioPeon,
        String.format("%s.complex_column", filenameBase),
        byteOrder,
        compression,
        serde
    );
    writer.open();
  }

  @Override
  public void serialize(Object obj) throws IOException
  {
    writer.add(obj);
  }

  @Override
  public long getSerializedSize()
  {
    return 0;
  }

  @Override
  public void close() throws IOException
  {
    writer.close();
  }

  @Override
  public void writeToChannel(WritableByteChannel channel) throws IOException
  {
    writer.writeToChannel(channel);
  }
}
