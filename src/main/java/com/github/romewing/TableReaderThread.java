package com.github.romewing;

import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;

public class TableReaderThread implements Callable<List<Map<String, Object>>> {

    private TableReader reader;


    @Override
    public List<Map<String, Object>> call() throws Exception {
        return reader.read();
    }
}
