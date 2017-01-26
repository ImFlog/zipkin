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

import com.squareup.moshi.JsonAdapter;
import com.squareup.moshi.JsonReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import okio.BufferedSource;

import static zipkin.moshi.JsonReaders.enterPath;

class SearchResultConverter<T> implements HttpCall.BodyConverter<List<T>> {
  final JsonAdapter<T> adapter;
  List<T> defaultValue = Collections.emptyList();

  SearchResultConverter(JsonAdapter<T> adapter) {
    this.adapter = adapter;
  }

  SearchResultConverter<T> defaultToNull() {
    defaultValue = null;
    return this;
  }

  @Override public List<T> convert(BufferedSource content) throws IOException {
    JsonReader hits = enterPath(JsonReader.of(content), "hits", "hits");
    if (hits == null || hits.peek() != JsonReader.Token.BEGIN_ARRAY) return defaultValue;

    List<T> result = new ArrayList<>();
    hits.beginArray();
    while (hits.hasNext()) {
      JsonReader source = enterPath(hits, "_source");
      if (source != null) {
        result.add(adapter.fromJson(source));
      }
      hits.endObject();
    }
    hits.endArray();
    return result.isEmpty() ? defaultValue : result;
  }
}