package com.unn.datacenter;

import com.unn.datacenter.service.DataController;
import retrofit2.GsonConverterFactory;
import retrofit2.Retrofit;

public class Main {

    public static void main(String[] args) {
        DataController.serve();

        Retrofit retrofit = new Retrofit.Builder()
                .baseUrl("https://api.github.com/")
                .addConverterFactory(GsonConverterFactory.create())
                .build();
    }
}
