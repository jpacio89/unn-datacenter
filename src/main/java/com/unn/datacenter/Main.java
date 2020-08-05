package com.unn.datacenter;

import com.unn.datacenter.service.DataController;
import org.postgresql.Driver;
import retrofit2.GsonConverterFactory;
import retrofit2.Retrofit;

import java.sql.*;

public class Main implements DriverAction {

    public static void main(String[] args) {
        DataController.serve();
    }

    @Override
    public void deregister() {

    }
}
