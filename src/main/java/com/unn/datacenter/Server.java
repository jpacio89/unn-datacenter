package com.unn.datacenter;

import com.google.gson.Gson;
import com.unn.common.dataset.Dataset;
import com.unn.common.dataset.DatasetDescriptor;
import com.unn.common.globals.NetworkConfig;
import com.unn.common.server.StandardResponse;
import com.unn.common.server.StatusResponse;
import com.unn.common.utils.CSVHelper;
import com.unn.datacenter.service.DataService;
import retrofit2.Call;
import retrofit2.http.GET;
import retrofit2.http.Path;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import static spark.Spark.*;

public class Server {
    static final String SUCCESS = new Gson().toJson(new StandardResponse(StatusResponse.SUCCESS));
    static DataService service;

    public Server() { }


    public static void serve() {
        service = new DataService();
        service.init();

        port(NetworkConfig.DATACENTER_PORT);

        // Resets brain
        post("/brain/reset", (request, response) -> {
            service.reset();
            return SUCCESS;
        });

        // Register agent and dataset (if not output agent) in the database and adds to listeners list
        post("/dataset/register", (request, response) -> {
            System.out.println(request.body());
            DatasetDescriptor descriptor = new Gson().fromJson(request.body(), DatasetDescriptor.class);
            service.registerAgent(descriptor);
            return SUCCESS;
        });

        // Store a dataset in the database and notifies listeners
        post("/dataset/:namespace/store/raw", (request, response) -> {
            String namespace = request.params("namespace");
            Dataset dataset = new CSVHelper().parse(request.body());
            dataset.getDescriptor().withNamespace(namespace);
            service.saveDataset(dataset);
            return SUCCESS;
        });

        // Get list of random features for mining or transformations
        get("/dataset/features/random/layer/:layer", (request, response) -> {
            int layer = Integer.parseInt(request.params("layer"));
            String[] whitelist = request.queryParamsValues("whitelist");
            Integer count = request.queryParams("count") != null ? Integer.parseInt(request.queryParams("count")) : null;
            HashMap<String, List<String>> ret = service.getRandomFeatures(layer, count, whitelist);
            return new Gson().toJson(ret);
        });

        get("/dataset/namespaces", (request, response) -> {
            ArrayList<DatasetDescriptor> ret = service.getNamespaces();
            return new Gson().toJson(ret);
        });

        // Get dataset for training, testing or transformation
        post("/agent/:agent/dataset/body", (request, response) -> {
            String agentType = request.params("agent");
            HashMap<String, List<String>> options = new Gson().fromJson(request.body(), HashMap.class);
            ArrayList<Integer> blacklistTimes = null;
            if ("miner".equals(agentType)) {
                blacklistTimes = service.getMiningBlacklistTimes(options.keySet());
            }
            Dataset dataset = service.getDatasetBodyByPurpose(options, agentType, null, blacklistTimes);
            return new CSVHelper().toString(dataset);
        });

        // Get list of random features for mining or transformations
        get("/dataset/:namespace/body/unpredicted", (request, response) -> {
            String namespace = request.params("namespace");
            Dataset dataset = service.getUnpredicted(namespace);
            return new CSVHelper().toString(dataset);
        });

        get("/dataset/:namespace/data", (request, response) -> {
            String namespace = request.params("namespace");
            int fromPrimer = Integer.parseInt(request.queryParams("fromPrimer"));
            Dataset dataset = service.getData(namespace, fromPrimer);
            return new CSVHelper().toString(dataset);
        });


    }

}
