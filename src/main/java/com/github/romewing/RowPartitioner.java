package com.github.romewing;

import org.springframework.batch.core.partition.support.Partitioner;
import org.springframework.batch.core.partition.support.SimplePartitioner;
import org.springframework.batch.item.ExecutionContext;

import java.util.HashMap;
import java.util.Map;

public class RowPartitioner implements Partitioner {

    private static final String PARTITION_KEY = "partition_";

    private static final String KEY_NAME = "partition";

    @Override
    public Map<String, ExecutionContext> partition(int gridSize) {
        Map<String, ExecutionContext> map = new HashMap<String, ExecutionContext>(gridSize);
        for (int i = 0; i < gridSize; i++) {
            ExecutionContext context = new ExecutionContext();
            context.putInt(KEY_NAME, i);
            map.put(PARTITION_KEY + i, context);
        }
        return map;
    }
}
