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

import com.google.common.collect.Maps;
import io.druid.segment.column.ValueType;
import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;
import java.util.HashMap;
import java.util.Map;

/**
 */
public class GenericIndexedTest
{
  @Test(expected = UnsupportedOperationException.class)
  public void testNotSortedNoIndexOf() throws Exception
  {
    GenericIndexed.fromArray(new String[]{"a", "c", "b"}, GenericIndexed.STRING_STRATEGY).indexOf("a");
  }

  @Test(expected = UnsupportedOperationException.class)
  public void testSerializationNotSortedNoIndexOf() throws Exception
  {
    serializeAndDeserialize(
        GenericIndexed.fromArray(
            new String[]{"a", "c", "b"}, GenericIndexed.STRING_STRATEGY
        ),
        ValueType.STRING
    ).indexOf("a");
  }

  @Test
  public void testSanity() throws Exception
  {
    final String[] strings = {"a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k", "l"};
    Indexed indexed = GenericIndexed.fromArray(strings, GenericIndexed.STRING_STRATEGY);

    checkBasicAPIs(strings, indexed, true);

    Assert.assertEquals(-13, indexed.indexOf("q"));
    Assert.assertEquals(-9, indexed.indexOf("howdydo"));
    Assert.assertEquals(-1, indexed.indexOf("1111"));
  }

  @Test
  public void testSortedSerialization() throws Exception
  {
    final String[] strings = {"a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k", "l"};

    GenericIndexed deserialized = serializeAndDeserialize(
        GenericIndexed.fromArray(
            strings, GenericIndexed.STRING_STRATEGY
        ),
        ValueType.STRING
    );

    checkBasicAPIs(strings, deserialized, true);

    Assert.assertEquals(-13, deserialized.indexOf("q"));
    Assert.assertEquals(-9, deserialized.indexOf("howdydo"));
    Assert.assertEquals(-1, deserialized.indexOf("1111"));
  }

  @Test
  public void testSortedSerializationFloat() throws Exception
  {
    final Float[] floats = {1.1f, 1.2f, 1.3f, 1.4f, 1.5f, 1.6f, 1.7f, 1.8f, 1.9f, 2.0f};

    GenericIndexed deserialized = serializeAndDeserialize(
        GenericIndexed.fromArray(
            floats, GenericIndexed.FLOAT_STRATEGY
        ),
        ValueType.FLOAT
    );
    checkBasicAPIs(floats, deserialized, true);

    Assert.assertEquals(-11, deserialized.indexOf(3.0f));
    Assert.assertEquals(-6, deserialized.indexOf(1.55f));
    Assert.assertEquals(-1, deserialized.indexOf(1.0f));
  }

  @Test
  public void testNotSortedSerialization() throws Exception
  {
    final String[] strings = {"a", "b", "c", "d", "e", "f", "g", "h", "i", "k", "j", "l"};

    GenericIndexed deserialized = serializeAndDeserialize(
        GenericIndexed.fromArray(
            strings, GenericIndexed.STRING_STRATEGY
        ),
        ValueType.STRING
    );
    checkBasicAPIs(strings, deserialized, false);
  }

  @Test
  public void testNotSortedSerializationFloat() throws Exception
  {
    final Float[] floats = {1.1f, 1.2f, 1.3f, 1.4f, 1.5f, 1.6f, 1.7f, 1.9f, 1.8f, 2.0f};

    GenericIndexed deserialized = serializeAndDeserialize(
        GenericIndexed.fromArray(
            floats, GenericIndexed.FLOAT_STRATEGY
        ),
        ValueType.FLOAT
    );
    checkBasicAPIs(floats, deserialized, false);
  }

  private void checkBasicAPIs(Comparable[] values, Indexed<Comparable> index, boolean allowReverseLookup)
  {
    Assert.assertEquals(values.length, index.size());
    for (int i = 0; i < values.length; i++) {
      Assert.assertEquals(values[i], index.get(i));
    }

    if (allowReverseLookup) {
      HashMap<Comparable, Integer> mixedUp = Maps.newHashMap();
      for (int i = 0; i < values.length; i++) {
        mixedUp.put(values[i], i);
      }
      for (Map.Entry<Comparable, Integer> entry : mixedUp.entrySet()) {
        Assert.assertEquals(entry.getValue().intValue(), index.indexOf(entry.getKey()));
      }
    } else {
      try {
        index.indexOf("xxx");
        Assert.fail("should throw exception");
      }
      catch (UnsupportedOperationException e) {
        // not supported
      }
    }
  }

  private GenericIndexed serializeAndDeserialize(GenericIndexed indexed, ValueType type) throws IOException
  {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    final WritableByteChannel channel = Channels.newChannel(baos);
    indexed.writeToChannel(channel);
    channel.close();

    final ByteBuffer byteBuffer = ByteBuffer.wrap(baos.toByteArray());
    Assert.assertEquals(indexed.getSerializedSize(), byteBuffer.remaining());
    GenericIndexed deserialized = GenericIndexed.read(
        byteBuffer, GenericIndexed.getObjectStrategy(type)
    );
    Assert.assertEquals(0, byteBuffer.remaining());
    return deserialized;
  }
}
