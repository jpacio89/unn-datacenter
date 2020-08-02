package com.unn.datacenter.service;

import retrofit2.GsonConverterFactory;
import retrofit2.Retrofit;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

public class AgentNotifier {
    HashMap<String, HashSet<String>> queue;

    public AgentNotifier() {
        this.queue = new HashMap<String, HashSet<String>>();
    }

    public void enqueue(String triggerDataset, String receivingDataset) {
        if (this.queue.containsKey(receivingDataset)) {
            this.queue.put(receivingDataset, new HashSet<>());
        }
        HashSet<String> set = this.queue.get(receivingDataset);
        set.add(triggerDataset);
    }

    public void dispatch() {
        Set<String> keys = this.queue.keySet();
        for (String key : keys) {
            if (!this.canDispatch(key)) {
                continue;
            }
        }
        Retrofit retrofit = new Retrofit.Builder()
            .baseUrl("https://api.github.com/")
            .addConverterFactory(GsonConverterFactory.create())
            .build();
        // TODO: implement HTTP call
    }

    private boolean canDispatch(String key) {
        // TODO: implement
        return true;
    }
}
