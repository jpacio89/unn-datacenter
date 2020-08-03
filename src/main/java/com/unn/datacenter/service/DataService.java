package com.unn.datacenter.service;

import com.unn.datacenter.models.Dataset;
import com.unn.datacenter.storage.PostgresExecutor;

// TODO: service that will save stuff in the storage and also notify listeners when new data is available
public class DataService {
    PostgresExecutor executor;
    AgentNotifier notifier;

    public DataService() {

    }

    public void init() {
        this.executor = new PostgresExecutor();
        this.notifier = new AgentNotifier();
    }

    public void saveDataset(Dataset dataset) {
        Dataset annotated = this.executor.annotateDataset(dataset);
        this.executor.storeDataset(annotated);
        for (String dependency : annotated.getDescriptor()
                .getDownstreamDependencies()) {
            this.notifier.enqueue(annotated.getDescriptor().getName(),
                    dependency);
        }
        this.notifier.dispatch();
    }
}
