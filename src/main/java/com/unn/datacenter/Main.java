package com.unn.datacenter;

import com.unn.datacenter.service.DataController;
import org.postgresql.Driver;
import retrofit2.GsonConverterFactory;
import retrofit2.Retrofit;

import java.sql.*;

public class Main implements DriverAction {

    public static void main(String[] args) {
        DataController.serve();

        try {
            // Creating driver instance
            Driver driver = new org.postgresql.Driver();
            // Creating Action Driver
            DriverAction da = new Main();
            // Registering driver by passing driver and driverAction
            DriverManager.registerDriver(driver, da);
            // Creating connection
            Connection con=DriverManager.getConnection("jdbc:mysql://localhost:3306/student","root","mysql");
            //Here student is database name, root is username and password is mysql
            Statement stmt=con.createStatement();
            // Executing SQL query
            ResultSet rs=stmt.executeQuery("select * from user");
            while(rs.next()){
                System.out.println(rs.getInt(1)+""+rs.getString(2)+""+rs.getString(3));
            }
            // Closing connection
            con.close();
            // Calling deregisterDriver method
            DriverManager.deregisterDriver(driver);
        }
        catch (Exception e){
            System.out.println(e);
        }
    }

    @Override
    public void deregister() {

    }
}
