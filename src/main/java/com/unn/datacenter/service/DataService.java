package com.unn.datacenter.service;

import com.unn.datacenter.models.Dataset;
import com.unn.datacenter.storage.PostgresExecutor;
import com.unn.datacenter.utils.RandomManager;

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
        for (int i = 0; i < count; ++i) {
            int rand = RandomManager.rand(1, accumulator);
            this.executor.getRandomFeatures(rand);
            accumulator -= rand;
            if (accumulator <= 0) {
                break;
            }
        }


    }
}
