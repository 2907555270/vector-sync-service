package com.example.vectorsync.infra.config;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.json.jackson.JacksonJsonpMapper;
import co.elastic.clients.transport.ElasticsearchTransport;
import co.elastic.clients.transport.rest_client.RestClientTransport;
import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class ElasticsearchConfig {

    @Value("${spring.elasticsearch.uris:http://localhost:9200}")
    private String elasticsearchUris;

    @Bean
    public RestClient restClient() {
        String[] uris = elasticsearchUris.split(",");
        HttpHost[] hosts = new HttpHost[uris.length];
        for (int i = 0; i < uris.length; i++) {
            String uri = uris[i].trim();
            String scheme = uri.startsWith("https") ? "https" : "http";
            String hostPort = uri.replace(scheme + "://", "");
            String[] hostPortArr = hostPort.split(":");
            String host = hostPortArr[0];
            int port = hostPortArr.length > 1 ? Integer.parseInt(hostPortArr[1]) : (scheme.equals("https") ? 443 : 80);
            hosts[i] = new HttpHost(host, port, scheme);
        }
        return RestClient.builder(hosts).build();
    }

    @Bean
    public ElasticsearchTransport elasticsearchTransport(RestClient restClient) {
        return new RestClientTransport(restClient, new JacksonJsonpMapper());
    }

    @Bean
    public ElasticsearchClient elasticsearchClient(ElasticsearchTransport transport) {
        return new ElasticsearchClient(transport);
    }
}
