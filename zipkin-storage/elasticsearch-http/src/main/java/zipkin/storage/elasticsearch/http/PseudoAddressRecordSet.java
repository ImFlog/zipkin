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

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import okhttp3.Dns;
import okhttp3.HttpUrl;

import static zipkin.internal.Util.checkArgument;

/**
 * This returns a Dns provider that combines the IPv4 or IPv6 addresses from a supplied list of
 * urls, provided they are all http and share the same port.
 */
final class PseudoAddressRecordSet {

  static Dns create(List<String> urls, Dns actualDns) {
    Set<String> schemes = new LinkedHashSet<>();
    Set<String> hosts = new LinkedHashSet<>();
    Set<InetAddress> ipAddresses = new LinkedHashSet<>();
    Set<Integer> ports = new LinkedHashSet<>();

    for (String url : urls) {
      HttpUrl httpUrl = HttpUrl.parse(url);
      schemes.add(httpUrl.scheme());

      // Kick out if we can't cheaply read the address
      byte[] addressBytes = null;
      try {
        addressBytes = ipStringToBytes(httpUrl.host());
      } catch (RuntimeException e) {
      }

      if (addressBytes != null) {
        try {
          ipAddresses.add(InetAddress.getByAddress(addressBytes));
        } catch (UnknownHostException e) {
          hosts.add(httpUrl.host());
        }
      } else {
        hosts.add(httpUrl.host());
      }
      ports.add(httpUrl.port());
    }

    checkArgument(ports.size() == 1, "Only one port supported with multiple hosts %s", urls);
    checkArgument(schemes.size() == 1 && schemes.iterator().next().equals("http"),
        "Only http supported with multiple hosts %s", urls);

    if (hosts.isEmpty()) return new StaticDns(ipAddresses);
    return new ConcatenatingDns(ipAddresses, hosts, actualDns);
  }

  static final class StaticDns implements Dns {
    private final List<InetAddress> ipAddresses;

    StaticDns(Set<InetAddress> ipAddresses) {
      this.ipAddresses = new ArrayList<>(ipAddresses);
    }

    @Override public List<InetAddress> lookup(String hostname) {
      return ipAddresses;
    }

    @Override public String toString() {
      return "StaticDns(" + ipAddresses + ")";
    }
  }

  static final class ConcatenatingDns implements Dns {
    final Set<InetAddress> ipAddresses;
    final Set<String> hosts;
    final Dns actualDns;

    ConcatenatingDns(Set<InetAddress> ipAddresses, Set<String> hosts, Dns actualDns) {
      this.ipAddresses = ipAddresses;
      this.hosts = hosts;
      this.actualDns = actualDns;
    }

    @Override public List<InetAddress> lookup(String hostname) throws UnknownHostException {
      List<InetAddress> result = new ArrayList<>(ipAddresses.size() + hosts.size());
      result.addAll(ipAddresses);
      for (String host : hosts) {
        result.addAll(actualDns.lookup(host));
      }
      return result;
    }

    @Override public String toString() {
      return "ConcatenatingDns(" + ipAddresses + "," + hosts + ")";
    }
  }

  /**
   * Returns the {@link InetAddress#getAddress()} having the given string representation or null if
   * unable to parse.
   *
   * <p>This deliberately avoids all nameservice lookups (e.g. no DNS).
   *
   * @param ipString {@code String} containing an IPv4 or IPv6 string literal, e.g. {@code
   * "192.168.0.1"} or {@code "2001:db8::1"}
   */
  static byte[] ipStringToBytes(String ipString) {
    return InetAddresses.ipStringToBytes(ipString);
  }
}
