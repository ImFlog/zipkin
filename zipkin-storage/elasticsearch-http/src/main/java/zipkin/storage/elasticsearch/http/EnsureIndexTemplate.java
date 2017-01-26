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
import okhttp3.HttpUrl;
import okhttp3.Request;
import okhttp3.RequestBody;
import zipkin.internal.Lazy;

import static zipkin.moshi.JsonReaders.enterPath;
import static zipkin.storage.elasticsearch.http.ElasticsearchHttpStorage.APPLICATION_JSON;

final class EnsureIndexTemplate extends Lazy<Void> {

  final Lazy<HttpCall.Factory> lazyHttp;
  final String indexTemplateName;
  final String indexTemplate;
  final String allIndices;

  EnsureIndexTemplate(ElasticsearchHttpStorage es) {
    this.lazyHttp = es.lazyHttp;
    this.indexTemplateName = es.index + "_template"; // should be 1:1 with indices
    this.allIndices = new IndexNameFormatter(es.index).allIndices();
    this.indexTemplate = INDEX_TEMPLATE
        .replace("${__INDEX__}", es.index)
        .replace("${__NUMBER_OF_SHARDS__}", String.valueOf(es.indexShards))
        .replace("${__NUMBER_OF_REPLICAS__}", String.valueOf(es.indexReplicas))
        .replace("${__TRACE_ID_MAPPING__}", es.strictTraceId
            ? "{ KEYWORD }" : "{ \"type\": \"string\", \"analyzer\": \"traceId_analyzer\" }");
  }

  static final String INDEX_TEMPLATE = "{\n"
      + "  \"template\": \"${__INDEX__}-*\",\n"
      + "  \"settings\": {\n"
      + "    \"index.number_of_shards\": ${__NUMBER_OF_SHARDS__},\n"
      + "    \"index.number_of_replicas\": ${__NUMBER_OF_REPLICAS__},\n"
      + "    \"index.requests.cache.enable\": true,\n"
      + "    \"analysis\": {\n"
      + "      \"analyzer\": {\n"
      + "        \"traceId_analyzer\": {\n"
      + "          \"type\": \"custom\",\n"
      + "          \"tokenizer\": \"keyword\",\n"
      + "          \"filter\": \"traceId_filter\"\n"
      + "        }\n"
      + "      },\n"
      + "      \"filter\": {\n"
      + "        \"traceId_filter\": {\n"
      + "          \"type\": \"pattern_capture\",\n"
      + "          \"patterns\": [\"([0-9a-f]{1,16})$\"],\n"
      + "          \"preserve_original\": true\n"
      + "        }\n"
      + "      }\n"
      + "    }\n"
      + "  },\n"
      + "  \"mappings\": {\n"
      + "    \"_default_\": {\n"
      + "      \"dynamic_templates\": [\n"
      + "        {\n"
      + "          \"strings\": {\n"
      + "            \"mapping\": {\n"
      + "              KEYWORD,\n"
      + "              \"ignore_above\": 256\n"
      + "            },\n"
      + "            \"match_mapping_type\": \"string\",\n"
      + "            \"match\": \"*\"\n"
      + "          }\n"
      + "        },\n"
      + "        {\n"
      + "          \"value\": {\n"
      + "            \"match\": \"value\",\n"
      + "            \"mapping\": {\n"
      + "              \"match_mapping_type\": \"string\",\n"
      + "              KEYWORD,\n"
      + "              \"ignore_above\": 256,\n"
      + "              \"ignore_malformed\": true\n"
      + "            }\n"
      + "          }\n"
      + "        },\n"
      + "        {\n"
      + "          \"annotations\": {\n"
      + "            \"match\": \"annotations\",\n"
      + "            \"mapping\": {\n"
      + "              \"type\": \"nested\"\n"
      + "            }\n"
      + "          }\n"
      + "        },\n"
      + "        {\n"
      + "          \"binaryAnnotations\": {\n"
      + "            \"match\": \"binaryAnnotations\",\n"
      + "            \"mapping\": {\n"
      + "              \"type\": \"nested\"\n"
      + "            }\n"
      + "          }\n"
      + "        }\n"
      + "      ],\n"
      + "      \"_all\": {\n"
      + "        \"enabled\": false\n"
      + "      }\n"
      + "    },\n"
      + "    \"span\": {\n"
      + "      \"properties\": {\n"
      + "        \"traceId\": ${__TRACE_ID_MAPPING__},\n"
      + "        \"timestamp_millis\": {\n"
      + "          \"type\":   \"date\",\n"
      + "          \"format\": \"epoch_millis\"\n"
      + "        },\n"
      + "        \"annotations\": {\n"
      + "          \"type\": \"nested\"\n"
      + "        },\n"
      + "        \"binaryAnnotations\": {\n"
      + "          \"type\": \"nested\"\n"
      + "        }\n"
      + "      }\n"
      + "    }\n"
      + "  }\n"
      + "}";

  @Override protected Void compute() {
    String version = getVersion();
    String versionSpecificTemplate = versionSpecificTemplate(version);
    ensureTemplate(indexTemplateName, versionSpecificTemplate);
    return null;
  }

  String versionSpecificTemplate(String version) {
    if (version.startsWith("2")) {
      return indexTemplate
          .replace("KEYWORD", "\"type\": \"string\", \"index\": \"not_analyzed\"");
    } else if (version.startsWith("5")) {
      return indexTemplate
          .replace("KEYWORD", "\"type\": \"keyword\"")
          .replace("\"analyzer\": \"traceId_analyzer\" }",
              "\"fielddata\": \"true\", \"analyzer\": \"traceId_analyzer\" }");
    } else {
      throw new IllegalStateException("Elasticsearch 2.x and 5.x are supported, was: " + version);
    }
  }

  String getVersion() {
    HttpCall.Factory http = lazyHttp.get();
    Request getNode = new Request.Builder().url(http.baseUrl).tag("get-node").build();

    return http.execute(getNode, b -> {
      JsonReader version = enterPath(JsonReader.of(b), "version", "number");
      if (version == null) throw new IllegalStateException(".version.number not in response");
      return version.nextString();
    });
  }

  /**
   * This is a blocking call, used inside a lazy. That's because no writes should occur until the
   * template is available.
   */
  void ensureTemplate(String name, String indexTemplate) {
    HttpCall.Factory http = lazyHttp.get();
    HttpUrl templateUrl = http.baseUrl.newBuilder("_template").addPathSegment(name).build();

    Request getTemplate = new Request.Builder().url(templateUrl).tag("get-template").build();
    try {
      http.execute(getTemplate, b -> null);
    } catch (IllegalStateException e) { // TODO: handle 404 slightly more nicely
      Request updateTemplate = new Request.Builder()
          .url(templateUrl)
          .put(RequestBody.create(APPLICATION_JSON, indexTemplate))
          .tag("update-template").build();
      http.execute(updateTemplate, b -> null);
    }
  }
}
