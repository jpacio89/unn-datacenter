package com.unn.datacenter.storage;

import com.unn.datacenter.Config;
import com.unn.datacenter.models.Dataset;
import org.postgresql.Driver;

import java.sql.*;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;

public class PostgresExecutor implements DriverAction {
    final String FIND_BY_NAMESPACE = "select * from _datasets where namespace = ?";
    final String INSERT_DATASET = "insert into _datasets (namespace, key) values (?, ?)";
    final String INSERT_DEPENDENCY = "insert into _dependencies (upstream, downstream) values (?, ?)";
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

    public Dataset annotateDataset(Dataset dataset) {
        try {
            PreparedStatement stmt = this.conn.prepareStatement(FIND_BY_NAMESPACE);
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

    public Dataset registerDataset(Dataset dataset) {
        // NOTE: create entry in _datasets table and annotate dataset parameter
        UUID key = UUID.randomUUID();
        dataset.getDescriptor().withKey(key.toString());
        this.insertDataset(dataset);
        // NOTE: connecting namespace with upstream
        for (String upstream : dataset.getDescriptor().getUpstreamDependencies()) {
            this.insertDependency(upstream, dataset.getDescriptor().getNamespace());
        }
        return dataset;
    }

    public void insertDataset(Dataset dataset) {
        try {
            PreparedStatement stmt = this.conn.prepareStatement(INSERT_DATASET);
            stmt.setString(0, dataset.getDescriptor().getNamespace());
            stmt.setString(1, dataset.getDescriptor().getKey());
            stmt.execute();
        } catch (Exception e) {
            System.out.println(e);
        }
    }

    public void insertDependency(String namespaceSource, String namespaceTarget) {
        try {
            this.conn.
            PreparedStatement stmt = this.conn.prepareStatement(INSERT_DEPENDENCY);
            stmt.setString(0, namespaceSource);
            stmt.setString(1, namespaceTarget);
            stmt.execute();
        } catch (Exception e) {
            System.out.println(e);
        }
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

    public void storeDataset(Dataset dataset) {
        this.inserMultiple(dataset);
        // TODO: store dataset data
    }

    private void inserMultiple(Dataset dataset) {
        final int batchSize = 1000;
        PreparedStatement ps = null;
        String cols = String.join(",", dataset.getHeader().getNames());
        try {
            String[] template = new String[dataset.getBody().getValues().length];
            Arrays.fill(template, "?");
            String vals = String.join(",", template);
            String sql = "INSERT INTO ? (?) VALUES (" + vals + ")";
            ps = this.conn.prepareStatement(sql);

            int insertCount=0;
            for (String[] values : dataset.getBody().getValues()) {
                ps.setString(1, dataset.getDescriptor().getKey());
                ps.setString(2, cols);
                for (int j = 0; j < values.length; ++j) {
                    ps.setString(3+j, values[j]);
                }
                ps.addBatch();
                if (++insertCount % batchSize == 0) {
                    ps.executeBatch();
                }
            }
            ps.executeBatch();

        } catch (SQLException e) {
            e.printStackTrace();
            System.exit(1);
        }
        finally {
            try {
                ps.close();
                this.conn.close();
            } catch (SQLException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }
    }
}
