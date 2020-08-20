package com.unn.datacenter.service;

import com.google.gson.Gson;
import com.unn.datacenter.models.*;
import retrofit2.GsonConverterFactory;
import retrofit2.Retrofit;

import java.util.HashMap;
import java.util.List;

import static spark.Spark.get;
import static spark.Spark.post;

public class DataController {
    static final String SUCCESS = new Gson().toJson(new StandardResponse(StatusResponse.SUCCESS));
    static DataService service;

    public DataController() {
    }


    public static void serve() {
        service = new DataService();
        service.init();

        //post("/dataset/store/csv", (req, res) -> "Dataset loaded");
        //post("/dataset/store/mysql", (req, res) -> "Dataset loaded");
        //post --> /dataset/:datasetName/listen

        // Register agent and dataset (if not output agent) in the database and adds to listeners list
        post("/agent/register", (request, response) -> {
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
            Integer count = request.queryParams("count") != null ? Integer.parseInt(request.queryParams("count")) : null;
            HashMap<String, List<String>> ret = service.getRandomFeatures(layer, count);
            return new Gson().toJson(new StandardResponse(StatusResponse.SUCCESS, null, ret));
        });

        // Get dataset for training, testing or transformation
        get("/dataset/:namespace/agent/:agent/body", (request, response) -> {
            String agent = request.params("agent");
            String namespace = request.params("namespace");
            Body body = service.getDatasetBodyByPurpose(namespace, agent);
            return new Gson().toJson(new StandardResponse(StatusResponse.SUCCESS, null, body));
        });

        // Resets brain
        get("/brain/reset", (request, response) -> {
            service.reset();
            return SUCCESS;
        });

    }

}
