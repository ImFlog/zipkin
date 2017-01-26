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

import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.Set;
import okhttp3.HttpUrl;
import okhttp3.Request;
import okhttp3.RequestBody;
import okio.Buffer;
import zipkin.internal.Nullable;
import zipkin.storage.Callback;

import static zipkin.storage.elasticsearch.http.ElasticsearchHttpStorage.APPLICATION_JSON;

// See https://www.elastic.co/guide/en/elasticsearch/reference/current/docs-bulk.html
// exposed to re-use for testing writes of dependency links
abstract class HttpBulkIndexer<T> {
  final String typeName;
  final String tag;
  final HttpCall.Factory http;
  final String pipeline;
  final boolean flushOnWrites;

  // Mutated for each call to add
  final Buffer body = new Buffer();
  final Set<String> indices = new LinkedHashSet<>();

  HttpBulkIndexer(String typeName, ElasticsearchHttpStorage es) {
    this.typeName = typeName;
    tag = "index-" + typeName;
    http = es.lazyHttp.get();
    pipeline = es.pipeline;
    flushOnWrites = es.flushOnWrites;
  }

  void add(String index, T object, @Nullable String id) {
    writeIndexMetadata(index, id);
    writeDocument(object);

    if (flushOnWrites) indices.add(index);
  }

  void writeIndexMetadata(String index, @Nullable String id) {
    body.writeUtf8("{\"index\":{\"_index\":\"").writeUtf8(index).writeByte('"');
    body.writeUtf8(",\"_type\":\"").writeUtf8(typeName).writeByte('"');
    if (id != null) {
      body.writeUtf8(",\"_id\":\"").writeUtf8(id).writeByte('"');
    }
    body.writeUtf8("}}\n");
  }

  void writeDocument(T object) {
    body.write(toJsonBytes(object));
    body.writeByte('\n');
  }

  abstract byte[] toJsonBytes(T object);

  /** Creates a bulk request when there is more than one object to store */
  void execute(Callback<Void> callback) {
    HttpUrl url = pipeline != null
        ? http.baseUrl.newBuilder("_bulk").addQueryParameter("pipeline", pipeline).build()
        : http.baseUrl.resolve("_bulk");

    Request request = new Request.Builder().url(url).tag(tag)
        .post(RequestBody.create(APPLICATION_JSON, body.readByteString())).build();

    http.<Void>newCall(request, b -> {
      if (indices.isEmpty()) return null;
      Iterator<String> index = indices.iterator();
      StringBuilder indexString = new StringBuilder(index.next());
      while (index.hasNext()) {
        indexString.append(',').append(index.next());
      }
      ElasticsearchHttpStorage.flush(http, indexString.toString());
      return null;
    }).submit(callback);
  }
}
