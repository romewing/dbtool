package com.github.romewing;

import org.springframework.batch.core.partition.support.Partitioner;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.Map;

@Component
public class RowPartitioner implements Partitioner {

    private static final String PARTITION_KEY = "partition_";

    private static final String KEY_NAME = "partition";

    @Override
    public Map<String, ExecutionContext> partition(int gridSize) {
        Map<String, ExecutionContext> map = new HashMap<>(gridSize);
        for (int i = 0; i < gridSize; i++) {
            ExecutionContext context = new ExecutionContext();
            context.putInt(KEY_NAME, i);
            map.put(PARTITION_KEY + i, context);
        }
        return map;
    }
}
