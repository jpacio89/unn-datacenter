package com.unn.datacenter;

import retrofit2.GsonConverterFactory;
import retrofit2.Retrofit;

public class Main {

    public static void main(String[] args) {
        Retrofit retrofit = new Retrofit.Builder()
                .baseUrl("https://api.github.com/")
                .addConverterFactory(GsonConverterFactory.create())
                .build();
    }
}
