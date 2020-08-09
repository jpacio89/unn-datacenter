package com.unn.datacenter.service;

import com.google.gson.Gson;
import com.unn.datacenter.models.Dataset;
import com.unn.datacenter.models.DatasetDescriptor;
import com.unn.datacenter.models.StandardResponse;
import com.unn.datacenter.models.StatusResponse;
import retrofit2.GsonConverterFactory;
import retrofit2.Retrofit;

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
            DatasetDescriptor dataset = new Gson().fromJson(request.body(), DatasetDescriptor.class);
            service.registerAgent(dataset);
            return SUCCESS;
        });

        // Store a dataset in the database and notifies listeners
        post("/dataset/store/raw", (request, response) -> {
            Dataset dataset = new Gson().fromJson(request.body(), Dataset.class);
            service.saveDataset(dataset);
            return SUCCESS;
        });

        // Get list of random features for mining or transformations
        get("/dataset/features/random/layer/:layer", (request, response) -> {
            int layer = Integer.parseInt(request.params("layer"));
            Integer count = request.queryParams("count") != null ? Integer.parseInt(request.queryParams("count")) : null;
            service.getRandomFeatures(layer, count);
            return SUCCESS;
        });

        // Get dataset for training, testing or transformation
        get("/dataset/body/:purpose", (request, response) -> {
            int purpose = Integer.parseInt(request.params("purpose"));
            return SUCCESS;
        });

        // Resets brain
        get("/brain/reset", (request, response) -> {
            return SUCCESS;
        });

    }

}
