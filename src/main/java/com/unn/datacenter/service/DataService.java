package com.unn.datacenter.service;

import com.unn.datacenter.models.Dataset;
import com.unn.datacenter.models.DatasetDescriptor;
import com.unn.datacenter.storage.PostgresExecutor;
import com.unn.datacenter.utils.RandomManager;
import javafx.util.Pair;

import java.util.HashMap;
import java.util.List;

public class DataService {
    final int DEFAULT_RANDOM_FEATURES = 100;
    final int MAX_DATASET_COUNT_RANDOM_FEATURES = 3;
    PostgresExecutor executor;
    AgentNotifier notifier;

    public DataService() {

    }

    public void init() {
        this.executor = new PostgresExecutor();
        this.notifier = new AgentNotifier();
        this.executor.init();
    }

    public void saveDataset(Dataset dataset) {
        this.executor.annotateDataset(dataset.getDescriptor());
        this.executor.storeDataset(dataset);
        String[] downstream = dataset.getDescriptor().getDownstreamDependencies();
        for (String dependency : downstream) {
            this.notifier.enqueue(dataset.getDescriptor().getNamespace(), dependency);
        }
        this.notifier.dispatch();
    }

    public void getDatasetBodyByPurpose(String namespace, String purpose) {

    }

    public void registerAgent(DatasetDescriptor descriptor) {
        this.executor.registerDataset(descriptor);
    }

    public HashMap<String, List<String>> getRandomFeatures(int _layer, Integer _count) {
        HashMap<String, List<String>> ret = new HashMap<String, List<String>>();
        int count = _count == null ? DEFAULT_RANDOM_FEATURES : _count;
        int accumulator = count;
        for (int i = 0; i <= MAX_DATASET_COUNT_RANDOM_FEATURES; ++i) {
            int rand = accumulator;
            if (i < MAX_DATASET_COUNT_RANDOM_FEATURES) {
                rand = RandomManager.rand(1, accumulator);
            }
            Pair<String, List<String>> pair = this.executor.getRandomFeatures(_layer, rand);
            if (pair == null || ret.containsKey(pair.getKey())) {
                continue;
            }
            ret.put(pair.getKey(), pair.getValue());
            accumulator -= rand;
            if (accumulator <= 0) {
                break;
            }
        }
        return ret;
    }
}
