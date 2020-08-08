package com.unn.datacenter.service;

import com.unn.datacenter.models.Dataset;
import com.unn.datacenter.storage.PostgresExecutor;
import com.unn.datacenter.utils.RandomManager;
import javafx.util.Pair;

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
        Dataset annotated = this.executor.annotateDataset(dataset);
        this.executor.storeDataset(annotated);
        String[] downstream = annotated.getDescriptor().getDownstreamDependencies();
        for (String dependency : downstream) {
            this.notifier.enqueue(annotated.getDescriptor().getNamespace(), dependency);
        }
        this.notifier.dispatch();
    }

    public void getRandomFeatures(int _layer, Integer _count) {
        int count = _count == null ? DEFAULT_RANDOM_FEATURES : _count;
        int accumulator = count;
        for (int i = 0; i <= MAX_DATASET_COUNT_RANDOM_FEATURES; ++i) {
            int rand = accumulator;
            if (i < MAX_DATASET_COUNT_RANDOM_FEATURES) {
                rand = RandomManager.rand(1, accumulator);
            }
            Pair<?,?> pair = this.executor.getRandomFeatures(_layer, rand);
            // TODO: use pair
            accumulator -= rand;
            if (accumulator <= 0) {
                break;
            }
        }
    }
}
