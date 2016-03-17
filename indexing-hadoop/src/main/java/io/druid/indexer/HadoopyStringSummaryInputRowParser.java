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

package io.druid.indexer;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.metamx.common.IAE;
import com.metamx.common.Pair;
import com.metamx.common.logger.Logger;
import io.druid.data.input.InputRow;
import io.druid.data.input.impl.InputRowParser;
import io.druid.data.input.impl.ParseSpec;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

/**
 */
public class HadoopyStringSummaryInputRowParser implements InputRowParser<Object>
{
  private final StringInputRowSummaryParser parser;
  private final HadoopCustomStringDecoder decoder;
  private final String encoding;


  static final Logger log = new Logger(HadoopCustomStringDecoder.class);

  public HadoopyStringSummaryInputRowParser(
      @JsonProperty("parseSpec") ParseSpec parseSpec,
      @JsonProperty("encoding") String encoding,
      @JsonProperty("decoder") HadoopCustomStringDecoder decoder
  )
  {
    this.parser = new StringInputRowSummaryParser(parseSpec, encoding, decoder);
    this.decoder = decoder;
    this.encoding = encoding;
  }


  @JsonProperty
  public HadoopCustomDecoder getDecoder()
  {
    return decoder;
  }

  @JsonProperty
  public String getEncoding()
  {
    return encoding;
  }

  @Override
  public InputRow parse(Object input)
  {
    if (input instanceof Text) {
      return parser.parse(((Text) input).toString());
    } else if (input instanceof BytesWritable) {
      BytesWritable valueBytes = (BytesWritable) input;
      return parser.parse(ByteBuffer.wrap(valueBytes.getBytes(), 0, valueBytes.getLength()));
    } else if (input instanceof Pair) {
      Pair<Map<String,String>,Object> pair = (Pair<Map<String,String>,Object>)input;
      return this.parsePair((Pair)pair.lhs,pair.rhs);
    } else {
      throw new IAE("can't convert type [%s] to InputRow", input.getClass().getName());
    }
  }

  private InputRow parsePair(Pair pair, Object input)
  {
    if (input instanceof Text) {
      return parser.parsePair(pair, ((Text) input).toString());
    } else if (input instanceof BytesWritable) {
      BytesWritable valueBytes = (BytesWritable) input;
      return parser.parse(ByteBuffer.wrap(valueBytes.getBytes(), 0, valueBytes.getLength()));
    } else {
      throw new IAE("can't convert type [%s] to InputRow", input.getClass().getName());
    }
  }

  @JsonProperty
  @Override
  public ParseSpec getParseSpec()
  {
    return parser.getParseSpec();
  }

  @Override
  public InputRowParser withParseSpec(ParseSpec parseSpec)
  {
    return new HadoopyStringSummaryInputRowParser(parseSpec, encoding, decoder);
  }
}
