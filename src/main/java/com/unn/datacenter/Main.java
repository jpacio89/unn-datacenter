package com.unn.datacenter;

import retrofit2.GsonConverterFactory;
import retrofit2.Retrofit;

import static spark.Spark.*;

public class Main {

    public static void main(String[] args) {
        get("/hello", (req, res) -> "Hello World");

        Retrofit retrofit = new Retrofit.Builder()
                .baseUrl("https://api.github.com/")
                .addConverterFactory(GsonConverterFactory.create())
                .build();
    }
}
