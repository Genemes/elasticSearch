package com.genemes.elasticsearch.reader;

import org.springframework.batch.item.ItemReader;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;

@Component
public class DeleteDocumentsItemReader implements ItemReader<String> {

    @Value("${diretorio.geracao.arquivos}")
    private String DIRECTORY_PATH;
    private Queue<File> filesQueue;
    private Queue<String> cdrSessionIdsQueue;

    @PostConstruct
    public void init() {
        File directory = new File(DIRECTORY_PATH);
        if (directory.isDirectory()) {
            filesQueue = new LinkedList<>(Arrays.asList(directory.listFiles()));
            cdrSessionIdsQueue = new LinkedList<>();
        } else {
            throw new RuntimeException("Provided path is not a directory!");
        }
    }

    @Override
    public String read() throws IOException {
        while (true) {
            if (!cdrSessionIdsQueue.isEmpty()) {
                return cdrSessionIdsQueue.poll();
            } else if (!filesQueue.isEmpty()) {
                loadNextFile();
                if (!cdrSessionIdsQueue.isEmpty()) {
                    return cdrSessionIdsQueue.poll();
                }
            } else {
                return null;
            }
        }
    }


    private void loadNextFile() throws IOException {
        File nextFile = filesQueue.poll();
        if (nextFile != null && nextFile.isFile()) {
            try (BufferedReader reader = new BufferedReader(new FileReader(nextFile))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    List<String> ids = Arrays.asList(line.split(","));
                    cdrSessionIdsQueue.addAll(ids);
                }
            }
        }
    }
}
