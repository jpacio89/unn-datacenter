package com.unn.datacenter.storage;

import com.unn.datacenter.Config;
import com.unn.datacenter.models.Dataset;
import org.postgresql.Driver;

import java.sql.*;
import java.util.UUID;

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
        // NOTE: create entry in _datasets table and annotate dataset parameter
        UUID key = UUID.randomUUID();
        dataset.getDescriptor().withKey(key.toString());
        this.insertDataset(dataset);
        // NOTE: connecting namespace with upstream
        for (String upstream : dataset.getDescriptor().getUpstreamDependencies()) {
            this.insertDependency(upstream, dataset.getDescriptor().getNamespace());
        }
        return null;
    }

    public void insertDataset(Dataset dataset) {
        try {
            String sql = "insert into _datasets (namespace, key) values (?, ?)";
            PreparedStatement stmt = this.conn.prepareStatement(sql);
            stmt.setString(0, dataset.getDescriptor().getNamespace());
            stmt.setString(1, dataset.getDescriptor().getKey());
            stmt.execute();
        } catch (Exception e) {
            System.out.println(e);
        }
    }

    public void insertDependency(String namespaceSource, String namespaceTarget) {
        try {
            String sql = "insert into _dependencies (upstream, downstream) values (?, ?)";
            PreparedStatement stmt = this.conn.prepareStatement(sql);
            stmt.setString(0, namespaceSource);
            stmt.setString(1, namespaceTarget);
            stmt.execute();
        } catch (Exception e) {
            System.out.println(e);
        }
    }

    public Dataset annotateDataset(Dataset dataset) {
        try {
            String sql = "select * from _datasets where namespace = ?";
            PreparedStatement stmt = this.conn.prepareStatement(sql);
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
