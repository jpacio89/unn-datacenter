package com.unn.datacenter.storage;

import com.unn.datacenter.Config;
import com.unn.datacenter.models.Dataset;
import org.postgresql.Driver;

import java.sql.*;

public class PostgresExecutor implements DriverAction {
    Driver driver;
    Connection conn;

    public PostgresExecutor() {

    }

    public void init() {
        try {
            this.driver = new org.postgresql.Driver();
            DriverManager.registerDriver(driver, this);
            String connectionPath = String.format(
                "jdbc:postgresql://%s:%s/%s",
                Config.DB_HOST,
                Config.DB_PORT,
                Config.DB_NAME
            );
            this.conn = DriverManager.getConnection(connectionPath, Config.DB_USER, Config.DB_PASSWORD);
        }
        catch (Exception e) {
            System.out.println(e);
        }
    }

    public void storeDataset(Dataset annotated) {
        // TODO: implement
    }

    public Dataset registerDataset(Dataset dataset) {
        // TODO: create entry in _datasets table and annotate dataset parameter
        return null;
    }

    public Dataset annotateDataset(Dataset dataset) {
        try {
            PreparedStatement stmt = this.conn.prepareStatement("select * from _datasets where name = ?");
            stmt.setString(0, dataset.getDescriptor().getNamespace());
            ResultSet rs = stmt.executeQuery();
            while (rs.next()) {
                String key = rs.getString("key");
                String downStreamDepends = rs.getString("downstream_depends");
                dataset.getDescriptor()
                    .withDownstreamDependencies(downStreamDepends.split(","))
                    .withKey(key);
                return dataset;
            }
            return registerDataset(dataset);
        } catch (Exception e) {
            System.out.println(e);
        }
        return null;
    }

    @Override
    public void deregister() {
        try {
            this.conn.close();
            DriverManager.deregisterDriver(this.driver);
        }
        catch (Exception e) {
            System.out.println(e);
        }
    }
}
