package io.richard.event;

@FunctionalInterface
public interface WithPartition {

    String getPartitionKey();
}
