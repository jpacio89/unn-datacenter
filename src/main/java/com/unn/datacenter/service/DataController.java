package com.unn.datacenter.service;

import com.google.gson.Gson;
import com.unn.datacenter.models.Dataset;
import com.unn.datacenter.models.StandardResponse;
import com.unn.datacenter.models.StatusResponse;
import retrofit2.GsonConverterFactory;
import retrofit2.Retrofit;

import static spark.Spark.get;
import static spark.Spark.post;

public class DataController {
    static DataService service;

    public DataController() {
    }


    public static void serve() {
        service = new DataService();
        service.init();

        //post("/dataset/load/openml", (req, res) -> "Dataset loaded");
        //post("/dataset/load/csv", (req, res) -> "Dataset loaded");
        //post("/dataset/load/mysql", (req, res) -> "Dataset loaded");
        //post --> /dataset/:datasetName/listen

        post("/dataset/store/raw", (request, response) -> {
            Dataset dataset = new Gson().fromJson(request.body(), Dataset.class);
            service.saveDataset(dataset);
            return new Gson().toJson(new StandardResponse(StatusResponse.SUCCESS));
        });

    }

}
