package com.unn.datacenter.service;

import retrofit2.GsonConverterFactory;
import retrofit2.Retrofit;

import static spark.Spark.get;
import static spark.Spark.post;

public class DataController {

    public static void serve() {
        //post("/dataset/load/openml", (req, res) -> "Dataset loaded");
        //post("/dataset/load/csv", (req, res) -> "Dataset loaded");
        //post("/dataset/load/mysql", (req, res) -> "Dataset loaded");
        //post --> /dataset/:datasetName/listen
        post("/dataset/load/raw", (req, res) -> "Dataset loaded");

    }

}
