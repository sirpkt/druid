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

package io.druid.segment.data;

import com.google.common.base.Preconditions;
import com.google.common.io.ByteStreams;
import com.google.common.io.OutputSupplier;
import com.google.common.primitives.Ints;
import io.druid.collections.ResourceHolder;
import io.druid.collections.StupidResourceHolder;
import io.druid.segment.CompressedPools;
import io.druid.segment.serde.ComplexMetricSerde;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;

public class CompressedFixedSizeObjectSupplierSerializer
{
  public static CompressedFixedSizeObjectSupplierSerializer create(
      final IOPeon ioPeon,
      final String filenameBase,
      final ByteOrder order,
      final CompressedObjectStrategy.CompressionStrategy compression,
      final ComplexMetricSerde serde
  )
  {
    Preconditions.checkNotNull(serde.getMetricSize(), "Should be fixed size");

    return new CompressedFixedSizeObjectSupplierSerializer(
        new GenericIndexedWriter<ResourceHolder<ByteBuffer>>(
            ioPeon,
            filenameBase,
            CompressedByteBufferObjectStrategy.getBufferForOrder(
                order,
                compression,
                // it's tricky - assumes that object size is always the same because it is fixed size metric
                serde.getMetricSize()
            )
        ),
        compression,
        serde
    );
  }

  private final GenericIndexedWriter<ResourceHolder<ByteBuffer>> flattener;
  private final CompressedObjectStrategy.CompressionStrategy compression;
  private final ComplexMetricSerde serde;

  private int numInserted = 0;

  private ByteBuffer endBuffer;

  public CompressedFixedSizeObjectSupplierSerializer(
      GenericIndexedWriter<ResourceHolder<ByteBuffer>> flattener,
      CompressedObjectStrategy.CompressionStrategy compression,
      ComplexMetricSerde serde
  )
  {
    this.flattener = flattener;
    this.compression = compression;
    this.serde = serde;

    initBuffer();
  }

  private void initBuffer()
  {
    endBuffer = ByteBuffer.allocate(CompressedPools.BUFFER_SIZE);
    endBuffer.mark();
  }

  public void open() throws IOException
  {
    flattener.open();
  }

  public int size()
  {
    return numInserted;
  }

  public void add(Object metric) throws IOException
  {
    if (endBuffer.remaining() < serde.getMetricSize()) {
      endBuffer.rewind();
      flattener.write(StupidResourceHolder.create(endBuffer));
      initBuffer();
    }

    endBuffer.put(serde.toBytes(metric));
    ++numInserted;
  }

  public void closeAndConsolidate(OutputSupplier<? extends OutputStream> consolidatedOut) throws IOException
  {
    close();
    try (OutputStream out = consolidatedOut.getOutput()) {
      ByteStreams.copy(flattener.combineStreams(), out);
    }
  }

  public void close() throws IOException {
    endBuffer.limit(endBuffer.position());
    endBuffer.rewind();
    flattener.write(StupidResourceHolder.create(endBuffer));
    endBuffer = null;
    flattener.close();
  }

  public long getSerializedSize()
  {
    return 1 +              // version
        Ints.BYTES +     // elements num
        Ints.BYTES +     // metric size
        1 +              // compression id
        flattener.getSerializedSize();
  }

  public void writeToChannel(WritableByteChannel channel) throws IOException
  {
    channel.write(ByteBuffer.wrap(new byte[]{CompressedFloatsIndexedSupplier.version}));
    channel.write(ByteBuffer.wrap(Ints.toByteArray(numInserted)));
    channel.write(ByteBuffer.wrap(Ints.toByteArray(serde.getMetricSize())));
    channel.write(ByteBuffer.wrap(new byte[]{compression.getId()}));
    final ReadableByteChannel from = Channels.newChannel(flattener.combineStreams().getInput());
    ByteStreams.copy(from, channel);
  }
}
