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

import com.google.common.base.Supplier;
import com.google.common.io.Closeables;
import com.google.common.primitives.Ints;
import com.metamx.common.IAE;
import com.metamx.common.guava.CloseQuietly;
import io.druid.collections.ResourceHolder;
import io.druid.segment.CompressedPools;
import io.druid.segment.column.ComplexColumn;
import io.druid.segment.serde.ComplexMetricSerde;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.WritableByteChannel;

public class CompressedFixedSizeComplexesIndexedSupplier implements Supplier<ComplexColumn>
{
  public static final byte LZF_VERSION = 0x1;
  public static final byte version = 0x2;

  private final int totalSize;
  private final GenericIndexed<ResourceHolder<ByteBuffer>> baseBuffers;
  private final CompressedObjectStrategy.CompressionStrategy compression;
  private final ComplexMetricSerde serde;
  private final int objectSize;

  CompressedFixedSizeComplexesIndexedSupplier(
      int totalSize,
      GenericIndexed<ResourceHolder<ByteBuffer>> baseBuffers,
      CompressedObjectStrategy.CompressionStrategy compression,
      ComplexMetricSerde serde
  )
  {
    this.totalSize = totalSize;
    this.baseBuffers = baseBuffers;
    this.compression = compression;
    this.serde = serde;
    this.objectSize = serde.getMetricSize();
  }

  public int size()
  {
    return totalSize;
  }

  public static int getMaxObjectsInBuffer(int objectSize)
  {
    return CompressedPools.BUFFER_SIZE / objectSize;
  }

  @Override
  public ComplexColumn get()
  {
    return new CompressedIndexedFixedSizeComplexes();
  }

  public long getSerializedSize()
  {
    return 1 +              // version
        Ints.BYTES +     // elements num
        Ints.BYTES +     // metric size
        1 +              // compression id
        baseBuffers.getSerializedSize();
  }

  public void writeToChannel(WritableByteChannel channel) throws IOException
  {
    channel.write(ByteBuffer.wrap(new byte[]{CompressedFloatsIndexedSupplier.version}));
    channel.write(ByteBuffer.wrap(Ints.toByteArray(totalSize)));
    channel.write(ByteBuffer.wrap(Ints.toByteArray(objectSize)));
    channel.write(ByteBuffer.wrap(new byte[]{compression.getId()}));
    baseBuffers.writeToChannel(channel);
  }

  public CompressedFixedSizeComplexesIndexedSupplier convertByteOrder(ByteOrder order)
  {
    return new CompressedFixedSizeComplexesIndexedSupplier(
        totalSize,
        GenericIndexed.fromIterable(baseBuffers, CompressedByteBufferObjectStrategy.getBufferForOrder(order, compression, objectSize)),
        compression,
        serde
    );
  }

  GenericIndexed<ResourceHolder<ByteBuffer>> getBaseBuffers()
  {
    return baseBuffers;
  }

  public static CompressedFixedSizeComplexesIndexedSupplier fromByteBuffer(ByteBuffer buffer, ByteOrder order, ComplexMetricSerde serde)
  {
    byte versionFromBuffer = buffer.get();

    if (versionFromBuffer == version || versionFromBuffer == LZF_VERSION) {
      final int totalSize = buffer.getInt();
      final int objectsize = buffer.getInt();
      final CompressedObjectStrategy.CompressionStrategy compression = (versionFromBuffer == LZF_VERSION)
          ? CompressedObjectStrategy.CompressionStrategy.LZF
          : CompressedObjectStrategy.CompressionStrategy.forId(buffer.get());
      return new CompressedFixedSizeComplexesIndexedSupplier(
          totalSize,
          GenericIndexed.read(buffer, CompressedByteBufferObjectStrategy.getBufferForOrder(order, compression, objectsize)),
          compression,
          serde
      );
    }

    throw new IAE("Unknown version[%s]", versionFromBuffer);
  }

  private class CompressedIndexedFixedSizeComplexes implements ComplexColumn
  {
    final Indexed<ResourceHolder<ByteBuffer>> singleThreadedLongBuffers = baseBuffers.singleThreaded();

    int currIndex = -1;

    ResourceHolder<ByteBuffer> holder;
    ByteBuffer buffer;

    int maxObjectsInBuffer = getMaxObjectsInBuffer(objectSize);

    @Override
    public Class<?> getClazz()
    {
      return serde.getObjectStrategy().getClazz();
    }

    @Override
    public String getTypeName()
    {
      return serde.getTypeName();
    }

    @Override
    public Object getRowValue(int index)
    {
      final int bufferNum = index / maxObjectsInBuffer;
      final int bufferOffset = index % maxObjectsInBuffer;

      if (bufferNum != currIndex) {
        loadBuffer(bufferNum);
      }

      final int pos = buffer.position() + bufferOffset * objectSize;
      final byte[] buf = new byte[objectSize];
      for(int idx = 0; idx < objectSize; idx++) {
        buf[idx] = buffer.get(pos + idx);
      }
      return serde.fromBytes(buf, 0, objectSize);
    }

    protected void loadBuffer(int bufferNum)
    {
      CloseQuietly.close(holder);
      holder = singleThreadedLongBuffers.get(bufferNum);
      buffer = holder.get();
      currIndex = bufferNum;
    }

    @Override
    public String toString()
    {
      return "CompressedFixedSizeComplexesIndexedSupplier_Anonymous{" +
          "currIndex=" + currIndex +
          ", sizePer=" + getMaxObjectsInBuffer(objectSize) +
          ", numChunks=" + singleThreadedLongBuffers.size() +
          ", totalSize=" + totalSize +
          '}';
    }

    @Override
    public void close() throws IOException
    {
      Closeables.close(holder, false);
    }
  }
}
