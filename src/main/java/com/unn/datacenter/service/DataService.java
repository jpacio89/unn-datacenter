package com.unn.datacenter.service;

import com.unn.common.dataset.*;
import com.unn.common.transformers.TransformerDescriptor;
import com.unn.datacenter.storage.PostgresExecutor;
import com.unn.common.utils.RandomManager;
import javafx.util.Pair;

import java.util.*;
import java.util.stream.Collectors;

public class DataService {
    final int DEFAULT_RANDOM_FEATURES = 20;
    final int MAX_DATASET_COUNT_RANDOM_FEATURES = 2;
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
        //String[] downstream = dataset.getDescriptor().getDownstreamDependencies();
        //if (downstream != null) {
          //  for (String dependency : downstream) {
          //      this.notifier.enqueue(dataset.getDescriptor().getNamespace(), dependency);
          //  }
        //}
        this.notifier.dispatch();
    }

    public Dataset getDatasetBodyByPurpose(HashMap<String, List<String>> options, String agent,
        ArrayList<String> whitelistTimes, ArrayList<Integer> blacklistTimes) {
        int maxCount = 0;
        if ("miner".equals(agent)) {
            maxCount = 1000;
        } else if ("transformer".equals(agent)) {
            maxCount = 10000;
        } else if ("predictor".equals(agent)) {
            maxCount = 1000;
        }
        if (whitelistTimes != null && whitelistTimes.size() > maxCount) {
            whitelistTimes = whitelistTimes.stream()
                .limit(maxCount)
                .collect(Collectors.toCollection(ArrayList::new));
        }
        ArrayList<HashMap<String, ArrayList<String>>> bodies = new ArrayList<>();
        ArrayList<String> features = new ArrayList<>();
        features.add("id");
        features.add("primer");
        int i = 0;
        for (Map.Entry<String, List<String>> option : options.entrySet()) {
            HashMap<String, ArrayList<String>> dataset = this.executor.getDatasetBody(option.getKey(),
                option.getValue(), maxCount, whitelistTimes, blacklistTimes);
            bodies.add(dataset);
            features.addAll(option.getValue());
            if (i == 0) {
                whitelistTimes = dataset.keySet().stream()
                    .collect(Collectors.toCollection(ArrayList::new));
            }
            ++i;
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
        for (String primer : pivot.keySet()) {
            ArrayList<String> merged = new ArrayList<>();
            merged.addAll(pivot.get(primer));
            for (int i = 1; i < bodies.size(); ++i) {
                HashMap<String, ArrayList<String>> other = bodies.get(i);
                if (!other.containsKey(primer)) {
                    merged = null;
                    break;
                }
                ArrayList<String> withoutId = other.get(primer);
                withoutId.remove(0);
                withoutId.remove(0);
                merged.addAll(withoutId);
            }
            if (merged != null) {
                rows.add(new Row().withValues(merged.toArray(new String[merged.size()])));
            }
        }
        return new Body().withRows(rows.toArray(new Row[rows.size()]));
    }

    public void registerAgent(DatasetDescriptor descriptor) {
        this.executor.registerDataset(descriptor);
        if (descriptor.getMakerPrimers() != null) {
            this.executor.registerMakerPrimers(descriptor.getNamespace(),
                descriptor.getMakerPrimers());
        }
        this.executor.createNamespace(descriptor.getNamespace(), descriptor.getHeader().getNames());
    }

    public HashMap<String, List<String>> getRandomFeatures(int _layer, Integer _count, String[] whitelist) {
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
        for (String feature : whitelist) {
            Feature f = new Feature(feature);
            if (!ret.containsKey(f.getNamespace())) {
                ret.put(f.getNamespace(), new ArrayList<>());
            }
            if (!ret.get(f.getNamespace()).contains(f.getColumn())) {
                ret.get(f.getNamespace()).add(f.getColumn());
            }
        }
        return ret;
    }

    public void reset() {
        this.executor.reset();
    }

    public Dataset getUnpredicted(String namespace) {
        DatasetDescriptor descriptor = new DatasetDescriptor()
            .withNamespace(namespace);
        this.executor.annotateDataset(descriptor);
        HashMap<String, List<String>> opts = getOptions(descriptor);
        String[] upstreamNamespaces = opts.keySet().toArray(new String[opts.size()]);
        ArrayList<String> times = this.executor.getMissingTimes(namespace, upstreamNamespaces);
        // TODO: order and limit
        Dataset dataset = getDatasetBodyByPurpose(opts, "predictor", times, null);
        return dataset;
    }

    public ArrayList<DatasetDescriptor> getNamespaces() {
        return this.executor.getNamespaces();
    }

    private HashMap<String, List<String>> getOptions(DatasetDescriptor descriptor) {
        String[] features = descriptor.getUpstreamDependencies();
        HashMap<String, List<String>> options = new HashMap<>();
        for (String feature : features) {
            Feature f = new Feature(feature);
            if (!options.containsKey(f.getNamespace())) {
                options.put(f.getNamespace(), new ArrayList<>());
            }
            options.get(f.getNamespace()).add(f.getColumn());
        }
        return options;
    }

    public ArrayList<Integer> getMiningBlacklistTimes(Set<String> namespaces) {
        ArrayList<Integer> times = new ArrayList<>();
        namespaces.stream().forEach((namespace -> {
            ArrayList<Integer> selfTimes = this.executor.getNamespaceMakerPrimers(namespace);
            times.addAll(selfTimes);
            Set<String> parentNamespaces = this.executor.getParentNamespaces(namespace);
            if (parentNamespaces.size() > 0) {
                ArrayList<Integer> parentTimes = this.getMiningBlacklistTimes(parentNamespaces);
                times.addAll(parentTimes);
            }
        }));
        return times;
    }

    public Dataset getData(String namespace, int fromPrimer) {
        return this.executor.getDatasetByTime(namespace, fromPrimer);
    }

    public ArrayList<TransformerDescriptor> getTransformers() {
        return this.executor.getTransformers();
    }
}
