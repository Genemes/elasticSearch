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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.item.ItemReader;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.io.IOException;

@Component
public class CreateFilesItemReader implements ItemReader<CdrSessionId> {

    private static final Logger logger = LoggerFactory.getLogger(CreateFilesItemReader.class);

    private final RestHighLevelClient client;
    @Autowired
    private ObjectMapper objectMapper;
    private SearchHit[] searchHits;
    private int currentCount = 0;
    private int currentPage = 0;
    @Value("${spring.data.elasticsearch.index.name}")
    private String INDEX_NAME;
    @Value("${pagination.size}")
    private int PAGINATION_SIZE;

    @Autowired
    public CreateFilesItemReader(RestHighLevelClient client, ObjectMapper objectMapper) {
        this.client = client;
        this.objectMapper = objectMapper;
        logger.info("CreateFilesItemReader - Established connection to ElasticSearch.");
    }

    @PostConstruct
    private void init() throws IOException {
        fetchNextPage();
    }

    private void fetchNextPage() throws IOException {
        SearchRequest searchRequest = new SearchRequest(INDEX_NAME);
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(QueryBuilders.matchAllQuery());
        searchSourceBuilder.size(PAGINATION_SIZE);
        searchSourceBuilder.from(currentPage * PAGINATION_SIZE);
        searchRequest.source(searchSourceBuilder);

        try {
            SearchResponse response = client.search(searchRequest, RequestOptions.DEFAULT);
            searchHits = response.getHits().getHits();

            // Se não houver mais registros para ler, defina searchHits como vazio.
            if (searchHits.length == 0) {
                logger.info("CreateFilesItemReader - List Hits is empty");
                searchHits = null;
            }

            currentPage++;
            logger.info("CreateFilesItemReader - Current page size changed to: " + currentPage);
            System.out.println();
        } catch (IOException e) {
            logger.error("CreateFilesItemReader - Error fetching next page from ElasticSearch.", e);
            throw e;
        }
    }

    @Override
    public CdrSessionId read() {
        if (searchHits == null) {
            logger.info("CreateFilesItemReader - Processing completed.");
            return null;  // Não há mais registros para ler.
        }

        if (currentCount < searchHits.length) {
            SearchHit searchHit = searchHits[currentCount];
            currentCount++;
            logger.info("CreateFilesItemReader - Current count changed to: " + currentCount);
            try {
                String cdrSessionIdValue = objectMapper.readTree(searchHit.getSourceAsString())
                        .get("CDR_SESSION_ID")
                        .asText();
                return new CdrSessionId(cdrSessionIdValue);
            } catch (IOException e) {
                logger.error("CreateFilesItemReader - Error reading CDR_SESSION_ID from ElasticSearch result.", e);
            }
        } else {
            try {
                fetchNextPage();
                currentCount = 0;
                return read();
            } catch (IOException e) {
                logger.error("CreateFilesItemReader - Error reading next page from ElasticSearch.", e);
            }
        }
        return null;
    }
}
