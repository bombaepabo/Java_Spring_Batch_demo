package com.example.Spring_batch_kafka.partitioner;

import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.partition.support.Partitioner;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.Map;

@Component
@Slf4j
public class CustomerPartitioner implements Partitioner {

    @Override
    public Map<String, ExecutionContext> partition(int gridSize) {
        Map<String, ExecutionContext> result = new HashMap<>();

        int totalRecords = getTotalRecordCount();
        int recordsPerPartition = (totalRecords + gridSize - 1) / gridSize;

        log.info("Partitioning {} records into {} partitions", totalRecords, gridSize);

        for (int i = 0; i < gridSize; i++) {
            ExecutionContext context = new ExecutionContext();
            context.putInt("minValue", i * recordsPerPartition);
            context.putInt("maxValue", Math.min((i + 1) * recordsPerPartition, totalRecords));
            context.putInt("partition", i);
            context.putString("name", "partition-" + i);

            result.put("partition" + i, context);
            log.debug("Created partition {} with range [{}, {})", i,
                    context.getInt("minValue"), context.getInt("maxValue"));
        }

        return result;
    }

    private int getTotalRecordCount() {
        // Count from file or database
        return 1000;
    }
}
