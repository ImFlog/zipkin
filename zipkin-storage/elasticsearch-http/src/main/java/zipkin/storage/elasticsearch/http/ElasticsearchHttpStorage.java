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

import com.squareup.moshi.JsonReader;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import okhttp3.HttpUrl;
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okio.Buffer;
import zipkin.internal.Lazy;
import zipkin.storage.AsyncSpanConsumer;
import zipkin.storage.AsyncSpanStore;
import zipkin.storage.SpanStore;
import zipkin.storage.StorageAdapters;
import zipkin.storage.StorageComponent;

import static zipkin.internal.Util.checkNotNull;
import static zipkin.moshi.JsonReaders.enterPath;

public final class ElasticsearchHttpStorage implements StorageComponent {
  static final MediaType APPLICATION_JSON = MediaType.parse("application/json");

  public static Builder builder(OkHttpClient client) {
    return new Builder(client);
  }

  public static Builder builder() {
    return new Builder(new OkHttpClient());
  }

  public static final class Builder implements StorageComponent.Builder {
    final OkHttpClient client;
    Lazy<List<String>> hosts;
    String pipeline;
    boolean flushOnWrites;
    int maxRequests = 64;
    boolean strictTraceId = true;
    String index = "zipkin";
    int indexShards = 5;
    int indexReplicas = 1;

    Builder(OkHttpClient client) {
      this.client = checkNotNull(client, "client");
      hosts(Collections.singletonList("http://localhost:9200"));
    }

    /**
     * A list of elasticsearch nodes to connect to, in http://host:port or https://host:port
     * format. Defaults to "http://localhost:9200".
     */
    public Builder hosts(Lazy<List<String>> hosts) {
      this.hosts = checkNotNull(hosts, "hosts");
      return this;
    }

    /** Sets maximum in-flight requests from this process to any Elasticsearch host. Defaults to 64 */
    public Builder maxRequests(int maxRequests) {
      this.maxRequests = maxRequests;
      return this;
    }

    /**
     * Only valid when the destination is Elasticsearch 5.x. Indicates the ingest pipeline used
     * before spans are indexed. No default.
     *
     * <p>See https://www.elastic.co/guide/en/elasticsearch/reference/master/pipeline.html
     */
    public Builder pipeline(String pipeline) {
      this.pipeline = pipeline;
      return this;
    }

    /** Visible for testing */
    public Builder flushOnWrites(boolean flushOnWrites) {
      this.flushOnWrites = flushOnWrites;
      return this;
    }

    /** {@inheritDoc} */
    @Override public Builder strictTraceId(boolean strictTraceId) {
      this.strictTraceId = strictTraceId;
      return this;
    }

    /**
     * A List of elasticsearch hosts to connect to, in a transport-specific format.
     * For example, for the native client, this would default to "localhost:9300".
     */
    public Builder hosts(List<String> hosts) {
      checkNotNull(hosts, "hosts");
      return hosts(new Lazy<List<String>>() {
        @Override protected List<String> compute() {
          return hosts;
        }
      });
    }

    /**
     * The index prefix to use when generating daily index names. Defaults to zipkin.
     */
    public Builder index(String index) {
      this.index = checkNotNull(index, "index");
      return this;
    }

    /**
     * The number of shards to split the index into. Each shard and its replicas are assigned to a
     * machine in the cluster. Increasing the number of shards and machines in the cluster will
     * improve read and write performance. Number of shards cannot be changed for existing indices,
     * but new daily indices will pick up changes to the setting. Defaults to 5.
     *
     * <p>Corresponds to <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/index-modules.html">index.number_of_shards</a>
     */
    public Builder indexShards(int indexShards) {
      this.indexShards = indexShards;
      return this;
    }

    /**
     * The number of replica copies of each shard in the index. Each shard and its replicas are
     * assigned to a machine in the cluster. Increasing the number of replicas and machines in the
     * cluster will improve read performance, but not write performance. Number of replicas can be
     * changed for existing indices. Defaults to 1. It is highly discouraged to set this to 0 as it
     * would mean a machine failure results in data loss.
     *
     * <p>Corresponds to <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/index-modules.html">index.number_of_replicas</a>
     */
    public Builder indexReplicas(int indexReplicas) {
      this.indexReplicas = indexReplicas;
      return this;
    }

    @Override public ElasticsearchHttpStorage build() {
      return new ElasticsearchHttpStorage(this);
    }
  }

  final String pipeline;
  final boolean flushOnWrites;
  final int maxRequests;
  final boolean strictTraceId;
  final String index;
  final int indexShards;
  final int indexReplicas;

  final Lazy<HttpCall.Factory> lazyHttp;
  final EnsureIndexTemplate ensureTemplate;
  final IndexNameFormatter indexNameFormatter;

  ElasticsearchHttpStorage(Builder builder) {
    pipeline = builder.pipeline;
    flushOnWrites = builder.flushOnWrites;
    maxRequests = builder.maxRequests;
    strictTraceId = builder.strictTraceId;
    index = builder.index;
    indexShards = builder.indexShards;
    indexReplicas = builder.indexReplicas;
    indexNameFormatter = new IndexNameFormatter(builder.index);

    final Lazy<List<String>> lazyHosts = builder.hosts;
    OkHttpClient unconfiguredClient = builder.client;
    lazyHttp = new Lazy<HttpCall.Factory>() {
      @Override protected HttpCall.Factory compute() {
        List<String> hosts = lazyHosts.get();
        if (hosts.isEmpty()) throw new IllegalArgumentException("no hosts configured");
        OkHttpClient ok = hosts.size() == 1
            ? unconfiguredClient
            : unconfiguredClient.newBuilder()
                .dns(PseudoAddressRecordSet.create(hosts, unconfiguredClient.dns()))
                .build();
        ok.dispatcher().setMaxRequests(maxRequests);
        ok.dispatcher().setMaxRequestsPerHost(maxRequests);
        return new HttpCall.Factory(ok, HttpUrl.parse(lazyHosts.get().get(0)));
      }
    };
    ensureTemplate = new EnsureIndexTemplate(this);
  }

  @Override public SpanStore spanStore() {
    return StorageAdapters.asyncToBlocking(asyncSpanStore());
  }

  @Override public AsyncSpanStore asyncSpanStore() {
    ensureTemplate.get();
    return new ElasticsearchHttpSpanStore(this);
  }

  @Override public AsyncSpanConsumer asyncSpanConsumer() {
    ensureTemplate.get();
    return new ElasticsearchHttpSpanConsumer(this);
  }

  /** This is a blocking call, only used in tests. */
  void clear() throws IOException {
    String index = indexNameFormatter.allIndices();
    HttpCall.Factory http = lazyHttp.get();

    Request deleteRequest = new Request.Builder()
        .url(http.baseUrl.newBuilder().addPathSegment(index).build())
        .delete().tag("delete-index").build();

    http.execute(deleteRequest, b -> null);

    flush(http, index);
  }

  /** This is a blocking call, only used in tests. */
  static void flush(HttpCall.Factory factory, String index) throws IOException {
    Request flushRequest = new Request.Builder()
        .url(factory.baseUrl.newBuilder().addPathSegment(index).addPathSegment("_flush").build())
        .post(RequestBody.create(APPLICATION_JSON, ""))
        .tag("flush-index").build();

    factory.execute(flushRequest, b -> null);
  }

  /** This is blocking so that we can determine if the cluster is healthy or not */
  @Override public CheckResult check() {
    String index = indexNameFormatter.allIndices();
    HttpCall.Factory http = lazyHttp.get();

    Request request = new Request.Builder().url(http.baseUrl.resolve("/_cluster/health/" + index))
            .tag("get-cluster-health").build();

    try {
      return http.execute(request, b -> {
        b.request(Long.MAX_VALUE); // Buffer the entire body.
        Buffer body = b.buffer();
        JsonReader status = enterPath(JsonReader.of(body.clone()), "status");
        if (status == null) {
          throw new IllegalStateException("Health status couldn't be read " + body.readUtf8());
        }
        if ("RED".equalsIgnoreCase(status.nextString())) {
          throw new IllegalStateException("Health status is RED");
        }
        return CheckResult.OK;
      });
    } catch (RuntimeException e) {
      return CheckResult.failed(e);
    }
  }

  @Override public void close() throws IOException {
    // TODO close okhttp client if we initialized it
  }

  @Override public String toString() {
    return lazyHttp.get().baseUrl.toString();
  }
}
