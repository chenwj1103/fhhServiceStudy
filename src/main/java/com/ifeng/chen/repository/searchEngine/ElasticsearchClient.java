package com.ifeng.chen.repository.searchEngine;

import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by Chen Weijie on 2017/8/12.
 */
public class ElasticsearchClient {


    public static TransportClient getTransportCilent() {

        String clusterHosts = "10.21.7.32:9200";
        Settings settings = Settings.builder()
                .put("cluster.name", "elasticsearch")
                .put("client.transport.sniff", true)
                .build();


        List<InetSocketTransportAddress> list = new ArrayList<InetSocketTransportAddress>();
        try {
            list = parseHosts(clusterHosts);
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }


        TransportClient client = new PreBuiltTransportClient(settings);

        for (InetSocketTransportAddress address : list) {
            client.addTransportAddress(address);
        }


        return client;
    }


    /**
     * 获取集群地址
     *
     * @param clusterHosts 集群字符串
     * @return
     * @throws UnknownHostException
     */
    public static List<InetSocketTransportAddress> parseHosts(String clusterHosts) throws UnknownHostException {

        String[] hosts = clusterHosts.split(",");

        List<InetSocketTransportAddress> list = new ArrayList<InetSocketTransportAddress>();

        for (String host : hosts) {
            if (host == null || "".equals(host)) {
                continue;
            }

            String[] meta = host.split(":");

            InetAddress address = InetAddress.getByName(meta[0]);
            int port = Integer.valueOf(meta[1]);

            list.add(new InetSocketTransportAddress(
                    address,
                    port)
            );
        }
        return list;
    }


}
