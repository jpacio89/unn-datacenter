package com.unn.datacenter.storage;

import com.unn.datacenter.Config;
import com.unn.datacenter.models.Body;
import com.unn.datacenter.models.Dataset;
import com.unn.datacenter.models.Header;
import com.unn.datacenter.models.Row;
import com.unn.datacenter.utils.RandomManager;
import javafx.util.Pair;
import org.postgresql.Driver;

import java.sql.*;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;

public class PostgresExecutor implements DriverAction {
    final String FIND_BY_NAMESPACE = "select * from _datasets where namespace = ?";
    final String INSERT_DATASET = "insert into _datasets (namespace, key, layer, features) values (?, ?, ?)";
    final String INSERT_DEPENDENCY = "insert into _dependencies (upstream, downstream) values (?, ?)";
    final String FIND_DOWNSTREAM_DEPENDENCIES = "select * from _dependencies where upstream = ?";
    final String FIND_BY_LAYER = "select * from _datasets where layer = ? order by random() limit 1 offset 0";
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
            String namespace = dataset.getDescriptor().getNamespace();
            PreparedStatement stmt = this.conn.prepareStatement(FIND_BY_NAMESPACE);
            stmt.setString(0, namespace);
            ResultSet rs = stmt.executeQuery();
            while (rs.next()) {
                String key = rs.getString("key");
                int layer = rs.getInt("layer");
                dataset.getDescriptor()
                    .withKey(key)
                    .withLayer(layer)
                    .withDownstreamDependencies(this.getDownstreamDependencies(namespace));
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

    public String[] getDownstreamDependencies(String namespace) {
        try {
            PreparedStatement stmt = this.conn.prepareStatement(FIND_DOWNSTREAM_DEPENDENCIES);
            stmt.setString(0, namespace);
            ResultSet rs = stmt.executeQuery();
            int size = rs.getFetchSize();
            String[] depends = new String[size];
            int i = 0;
            while (rs.next()) {
                depends[i] = rs.getString("downstream");
                ++i;
            }
            return depends;
        } catch (Exception e) {
            System.out.println(e);
        }
        return null;
    }

    public void insertDataset(Dataset dataset) {
        try {
            PreparedStatement stmt = this.conn.prepareStatement(INSERT_DATASET);
            stmt.setString(0, dataset.getDescriptor().getNamespace());
            stmt.setString(1, dataset.getDescriptor().getKey());
            stmt.setInt(2, dataset.getDescriptor().getLayer());
            stmt.setString(3, String.join(",", dataset.getHeader().getNames()));
            stmt.execute();
        } catch (Exception e) {
            System.out.println(e);
        }
    }

    public void insertDependency(String namespaceSource, String namespaceTarget) {
        try {
            PreparedStatement stmt = this.conn.prepareStatement(INSERT_DEPENDENCY);
            stmt.setString(0, namespaceSource);
            stmt.setString(1, namespaceTarget);
            stmt.execute();
        } catch (Exception e) {
            System.out.println(e);
        }
    }

    public void storeDataset(Dataset dataset) {
        this.inserMultiple(dataset.getDescriptor().getKey(), dataset.getHeader(), dataset.getBody());
    }

    private void inserMultiple(String table, Header header, Body body) {
        final int batchSize = 1000;
        PreparedStatement ps = null;
        try {
            String[] template = new String[body.getRows().length];
            Arrays.fill(template, "?");
            String sql = String.format(
                "INSERT INTO %s (%s) VALUES (%s)",
                table,
                String.join(",", header.getNames()),
                String.join(",", template)
            );
            ps = this.conn.prepareStatement(sql);
            int insertCount = 0;
            for (Row row : body.getRows()) {
                String[] values = row.getValues();
                for (int j = 0; j < values.length; ++j) {
                    ps.setString(j, values[j]);
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

    public Pair<String, List<String>> getRandomFeatures(int _layer, int rand) {
        try {
            PreparedStatement stmt = this.conn.prepareStatement(FIND_BY_LAYER);
            stmt.setInt(0, _layer);
            ResultSet rs = stmt.executeQuery();
            while (rs.next()) {
                String namespace = rs.getString("namespace");
                String[] features = rs.getString("features").split(",");
                List<String> selectedFeatures = (List<String>) RandomManager.getMany(features, rand);
                Pair<String, List<String>> ret = new Pair<>(namespace, selectedFeatures);
                return ret;
            }
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
