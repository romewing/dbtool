package com.github.romewing;

import org.springframework.batch.core.partition.support.Partitioner;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.Map;

@Component
public class DataPartitioner implements Partitioner{

    private static final String PARTITION_KEY = "partition";

    private static final String _INDEX = "_index";

    @Override
    public Map<String, ExecutionContext> partition(int gridSize) {
        Map<String, ExecutionContext> map = new HashMap<String, ExecutionContext>(gridSize);
        for (int i = 0; i < gridSize; i++) {
            ExecutionContext context = new ExecutionContext();
            context.putInt(_INDEX, i);
            map.put(PARTITION_KEY + i, context);
        }
        return map;
    }
}
