package com.genemes.elasticsearch.reader;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.genemes.elasticsearch.model.CdrSessionId;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.SearchHit;
import org.springframework.batch.item.ItemReader;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.io.IOException;

@Component
public class CreateFilesItemReader implements ItemReader<CdrSessionId> {

    private final RestHighLevelClient client;
    @Autowired
    private ObjectMapper objectMapper;
    private SearchHit[] searchHits;
    private int currentCount = 0;
    private int currentPage = 0;
    @Value("${spring.data.elasticsearch.index.name}")
    private String INDEX_NAME;

    @Autowired
    public CreateFilesItemReader(RestHighLevelClient client, ObjectMapper objectMapper) {
        this.client = client;
        this.objectMapper = objectMapper;
    }

    @PostConstruct
    private void init() throws IOException {
        fetchNextPage();
    }

    private void fetchNextPage() throws IOException {
        SearchRequest searchRequest = new SearchRequest("tslee-cdr20-2023.10.25");
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(QueryBuilders.matchAllQuery());
        searchSourceBuilder.size(100);
        searchSourceBuilder.from(currentPage * 100);
        searchRequest.source(searchSourceBuilder);

        SearchResponse response = client.search(searchRequest, RequestOptions.DEFAULT);
        searchHits = response.getHits().getHits();

        // Se não houver mais registros para ler, defina searchHits como vazio.
        if (searchHits.length == 0) {
            searchHits = null;
        }

        currentPage++;
    }

    @Override
    public CdrSessionId read() {
        if (searchHits == null) {
            return null;  // Não há mais registros para ler.
        }

        if (currentCount < searchHits.length) {
            SearchHit searchHit = searchHits[currentCount];
            currentCount++;
            try {
                String cdrSessionIdValue = objectMapper.readTree(searchHit.getSourceAsString())
                        .get("CDR_SESSION_ID")
                        .asText();
                return new CdrSessionId(cdrSessionIdValue);
            } catch (IOException e) {
                e.printStackTrace();
            }
        } else {
            try {
                fetchNextPage();
                currentCount = 0;
                return read();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return null;
    }
}

