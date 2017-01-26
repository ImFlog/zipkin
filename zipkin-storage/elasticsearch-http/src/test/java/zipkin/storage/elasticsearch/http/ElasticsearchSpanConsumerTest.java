/**
 * Copyright 2015-2017 The OpenZipkin Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package zipkin.storage.elasticsearch.http;

import java.io.IOException;
import java.util.List;
import okhttp3.Call;
import okhttp3.HttpUrl;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import org.junit.Before;
import org.junit.Test;
import zipkin.Annotation;
import zipkin.Codec;
import zipkin.Span;
import zipkin.internal.CallbackCaptor;
import zipkin.internal.Util;
import zipkin.storage.Callback;

import static java.util.Arrays.asList;
import static org.assertj.core.api.Assertions.assertThat;
import static zipkin.Constants.SERVER_RECV;
import static zipkin.Constants.SERVER_SEND;
import static zipkin.TestObjects.DAY;
import static zipkin.TestObjects.TODAY;
import static zipkin.TestObjects.WEB_ENDPOINT;

public abstract class ElasticsearchSpanConsumerTest {

  /** Should maintain state between multiple calls within a test. */
  protected abstract ElasticsearchHttpStorage storage();

  /** Clears store between tests. */
  @Before
  public void clear() throws IOException {
    storage().clear();
  }

  @Test
  public void spanGoesIntoADailyIndex_whenTimestampIsDerived() throws Exception {
    long twoDaysAgo = (TODAY - 2 * DAY);

    Span span = Span.builder().traceId(20L).id(20L).name("get")
        .addAnnotation(Annotation.create(twoDaysAgo * 1000, SERVER_RECV, WEB_ENDPOINT))
        .addAnnotation(Annotation.create(TODAY * 1000, SERVER_SEND, WEB_ENDPOINT))
        .build();

    accept(span);

    CallbackCaptor<List<Span>> indexFromTwoDaysAgo = new CallbackCaptor<>();
    findSpans(twoDaysAgo, span.traceId, indexFromTwoDaysAgo);

    // make sure the span went into an index corresponding to its first annotation timestamp
    assertThat(indexFromTwoDaysAgo.get())
        .hasSize(1);
  }

  void findSpans(long endTs, long traceId, Callback<List<Span>> callback) {
    ((ElasticsearchHttpSpanStore) storage().asyncSpanStore()).findSpans(
        asList(storage().indexNameFormatter.indexNameForTimestamp(endTs)),
        asList(Util.toLowerHex(traceId)),
        callback
    );
  }

  @Test
  public void spanGoesIntoADailyIndex_whenTimestampIsExplicit() throws Exception {
    long twoDaysAgo = (TODAY - 2 * DAY);

    Span span = Span.builder().traceId(20L).id(20L).name("get")
        .timestamp(twoDaysAgo * 1000).build();

    accept(span);

    CallbackCaptor<List<Span>> indexFromTwoDaysAgo = new CallbackCaptor<>();
    findSpans(twoDaysAgo, span.traceId, indexFromTwoDaysAgo);

    // make sure the span went into an index corresponding to its timestamp, not collection time
    assertThat(indexFromTwoDaysAgo.get())
        .hasSize(1);
  }

  @Test
  public void spanGoesIntoADailyIndex_fallsBackToTodayWhenNoTimestamps() throws Exception {
    Span span = Span.builder().traceId(20L).id(20L).name("get").build();

    accept(span);

    CallbackCaptor<List<Span>> indexFromToday = new CallbackCaptor<>();
    findSpans(TODAY, span.traceId, indexFromToday);

    // make sure the span went into an index corresponding to collection time
    assertThat(indexFromToday.get())
        .hasSize(1);
  }

  @Test
  public void searchByTimestampMillis() throws Exception {
    Span span = Span.builder().timestamp(TODAY * 1000).traceId(20L).id(20L).name("get").build();

    accept(span);

    Call searchRequest = new OkHttpClient().newCall(new Request.Builder().url(
        HttpUrl.parse(baseUrl()).newBuilder()
            .addPathSegment(storage().indexNameFormatter.allIndices())
            .addPathSegment("span")
            .addPathSegment("_search")
            .addQueryParameter("q", "timestamp_millis:" + TODAY).build())
        .get().tag("search-terms").build());

    assertThat(searchRequest.execute().body().string())
        .contains("\"hits\":{\"total\":1");
  }

  abstract String baseUrl();

  @Test
  public void prefixWithTimestampMillis() {
    Span span = Span.builder().traceId(20L).id(20L).name("get")
        .timestamp(TODAY * 1000).build();

    byte[] result =
        HttpBulkSpanIndexer.prefixWithTimestampMillis(Codec.JSON.writeSpan(span), TODAY);

    String json = new String(result);
    assertThat(json)
        .startsWith("{\"timestamp_millis\":" + Long.toString(TODAY) + ",\"traceId\":");

    assertThat(Codec.JSON.readSpan(json.getBytes()))
        .isEqualTo(span); // ignores timestamp_millis field
  }

  void accept(Span span) throws Exception {
    CallbackCaptor<Void> callback = new CallbackCaptor<>();
    storage().asyncSpanConsumer().accept(asList(span), callback);
    callback.get();
  }
}
