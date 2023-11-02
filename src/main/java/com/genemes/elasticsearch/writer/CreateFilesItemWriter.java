package com.genemes.elasticsearch.writer;

import com.genemes.elasticsearch.model.CdrSessionId;
import org.springframework.batch.item.ItemWriter;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.util.List;
import java.util.stream.Collectors;

@Component
public class CreateFilesItemWriter implements ItemWriter<CdrSessionId> {
    private int currentFileCount = 1;
    @Value("${diretorio.geracao.arquivos.error}")
    private String DIRECTORY_PATH;

    @Override
    public void write(List<? extends CdrSessionId> items) throws Exception {
        String outputPath = DIRECTORY_PATH + "batch_" + currentFileCount + ".csv";

        try (BufferedWriter writer = new BufferedWriter(new FileWriter(outputPath))) {
            String line = items.stream()
                    .map(CdrSessionId::getValue)
                    .collect(Collectors.joining(","));
            writer.write(line);
            writer.newLine();
        }

        currentFileCount++;
    }
}