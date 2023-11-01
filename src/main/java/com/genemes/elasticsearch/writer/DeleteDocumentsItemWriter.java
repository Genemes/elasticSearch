package com.genemes.elasticsearch.writer;

import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.index.reindex.BulkByScrollResponse;
import org.elasticsearch.index.reindex.DeleteByQueryRequest;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.reindex.ScrollableHitSource;
import org.springframework.batch.item.ItemWriter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.util.List;
import java.util.stream.Collectors;

@Component
public class DeleteDocumentsItemWriter implements ItemWriter<String> {

    private final RestHighLevelClient client;
    private int currentFileCount = 1;

    @Value("${spring.data.elasticsearch.exclude.index.name}")
    private String INDEX_NAME;
    @Value("${spring.data.elasticsearch.slice}")
    private int SLICE;

    @Value("${diretorio.geracao.arquivos.error}")
    private String DIRECTORY_PATH;

    @Autowired
    public DeleteDocumentsItemWriter(RestHighLevelClient client) {
        this.client = client;
    }

    @Override
    public void write(List<? extends String> cdrSessionIds) throws Exception {
        for (String cdrSessionId : cdrSessionIds) {
            DeleteByQueryRequest deleteByQueryRequest = new DeleteByQueryRequest(INDEX_NAME);
            deleteByQueryRequest.setQuery(QueryBuilders.termQuery("CDR_SESSION_ID.keyword", cdrSessionId));
            deleteByQueryRequest.setSlices(SLICE);

            BulkByScrollResponse response = client.deleteByQuery(deleteByQueryRequest, RequestOptions.DEFAULT);

            List<ScrollableHitSource.SearchFailure> failures = response.getSearchFailures();
            if (!failures.isEmpty()) {
                String errorFilePath = DIRECTORY_PATH + "saida_" + currentFileCount + "-err.csv";

                try (BufferedWriter writer = new BufferedWriter(new FileWriter(errorFilePath, true))) {
                    String line = failures.stream()
                            .map(failure -> failure.getIndex() + ": " + failure.getReason())
                            .collect(Collectors.joining(","));
                    writer.write(line);
                    writer.newLine();
                }
            }
        }
        currentFileCount++;
    }

}
