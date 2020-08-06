package com.unn.datacenter.service;

import com.unn.datacenter.models.Dataset;
import com.unn.datacenter.storage.PostgresExecutor;

public class DataService {
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
}
