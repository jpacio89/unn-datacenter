package com.unn.datacenter.service;

import com.unn.datacenter.models.*;
import com.unn.datacenter.storage.PostgresExecutor;
import com.unn.datacenter.utils.RandomManager;
import javafx.util.Pair;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
        if (downstream != null) {
            for (String dependency : downstream) {
                this.notifier.enqueue(dataset.getDescriptor().getNamespace(), dependency);
            }
        }
        this.notifier.dispatch();
    }

    public Dataset getDatasetBodyByPurpose(HashMap<String, List<String>> options, String agent) {
        int maxCount = 0;
        if ("miner".equals(agent)) {
            maxCount = 1000;
        } else if ("transformer".equals(agent)) {
            maxCount = 10000;
        }
        ArrayList<HashMap<String, ArrayList<String>>> bodies = new ArrayList<>();
        ArrayList<String> features = new ArrayList<String>();
        features.add("id");
        for (Map.Entry<String, List<String>> option : options.entrySet()) {
            HashMap<String, ArrayList<String>> dataset = this.executor.getDatasetBody(option.getKey(), option.getValue(), maxCount);
            bodies.add(dataset);
            features.addAll(option.getValue());
        }
        Body merged = mergeBodies(bodies);

        return new Dataset()
            .withDescriptor(new DatasetDescriptor()
                .withHeader(new Header()
                    .withNames(features.toArray(new String[features.size()]))))
            .withBody(merged);
    }

    Body mergeBodies(ArrayList<HashMap<String, ArrayList<String>>> bodies) {
        HashMap<String, ArrayList<String>> pivot = bodies.get(0);
        ArrayList<Row> rows = new ArrayList<>();
        for (String key : pivot.keySet()) {
            ArrayList<String> merged = new ArrayList<>();
            merged.addAll(pivot.get(key));
            for (int i = 1; i < bodies.size(); ++i) {
                HashMap<String, ArrayList<String>> other = bodies.get(i);
                if (!other.containsKey(key)) {
                    merged = null;
                    break;
                }
                merged.addAll(other.get(key));
            }
            if (merged != null) {
                rows.add(new Row().withValues(merged.toArray(new String[merged.size()])));
            }
        }
        return new Body().withRows(rows.toArray(new Row[rows.size()]));
    }

    public void registerAgent(DatasetDescriptor descriptor) {
        this.executor.registerDataset(descriptor);
        this.executor.createTable(descriptor.getNamespace(), descriptor.getHeader().getNames());
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

    public void reset() {
        this.executor.reset();
    }
}
