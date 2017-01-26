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
import java.util.concurrent.TimeUnit;
import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import okhttp3.mockwebserver.RecordedRequest;
import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import zipkin.DependencyLink;
import zipkin.internal.CallbackCaptor;

import static java.util.Arrays.asList;
import static org.assertj.core.api.Assertions.assertThat;

public class ElasticsearchHttpSpanStoreTest {
  @Rule
  public ExpectedException thrown = ExpectedException.none();
  @Rule
  public MockWebServer es = new MockWebServer();

  ElasticsearchHttpSpanStore client = new ElasticsearchHttpSpanStore(
      ElasticsearchHttpStorage.builder()
          .hosts(asList(es.url("").toString()))
          .build());

  @After
  public void close() throws IOException {
    client.search.http.ok.dispatcher().executorService().shutdownNow();
  }

  /** Declaring queries alphabetically helps simplify amazon signature logic */
  @Test
  public void lenientSearchOrdersQueryAlphabetically() throws Exception {
    es.enqueue(new MockResponse());

    assertThat(client.search.lenientSearch(asList("zipkin-2016-10-01"), "span")
        .queryParameterNames())
        .containsExactly("allow_no_indices", "expand_wildcards", "ignore_unavailable");
  }

  @Test
  public void findDependencies() throws Exception {
    es.enqueue(new MockResponse());

    long endTs = client.indexNameFormatter.parseDate("2016-10-02");
    CallbackCaptor<List<DependencyLink>> callback = new CallbackCaptor<>();
    client.getDependencies(endTs, TimeUnit.DAYS.toMillis(1), callback);
    callback.get();

    RecordedRequest request = es.takeRequest();
    assertThat(request.getPath())
        .isEqualTo(
            "/zipkin-2016-10-01,zipkin-2016-10-02/dependencylink/_search?allow_no_indices=true&expand_wildcards=open&ignore_unavailable=true");
  }
}
