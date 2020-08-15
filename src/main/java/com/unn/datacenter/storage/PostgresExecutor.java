package com.unn.datacenter.storage;

import com.unn.datacenter.Config;
import com.unn.datacenter.models.*;
import com.unn.datacenter.utils.RandomManager;
import javafx.util.Pair;
import org.postgresql.Driver;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.*;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;

public class PostgresExecutor implements DriverAction {
    final String FIND_BY_NAMESPACE = "select * from @datasets where namespace = ?";
    final String INSERT_DATASET = "insert into @datasets (namespace, key, layer, features) values (?, ?, ?)";
    final String INSERT_DEPENDENCY = "insert into @dependencies (upstream, downstream) values (?, ?)";
    final String FIND_DOWNSTREAM_DEPENDENCIES = "select * from @dependencies where upstream = ?";
    final String FIND_BY_LAYER = "select * from @datasets where layer = ? order by random() limit 1 offset 0";
    final String FETCH_DATASET_BODY = "select * from %s order by random() limit %d limit 0";
    Driver driver;
    Connection conn;
    Boolean isInstalled;

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
            e.printStackTrace();
        }
    }

    public void annotateDataset(DatasetDescriptor descriptor) {
        PreparedStatement stmt = null;
        try {
            String namespace = descriptor.getNamespace();
            stmt = this.conn.prepareStatement(FIND_BY_NAMESPACE);
            stmt.setString(0, namespace);
            ResultSet rs = stmt.executeQuery();
            while (rs.next()) {
                String key = rs.getString("key");
                int layer = rs.getInt("layer");
                descriptor
                    .withKey(key)
                    .withLayer(layer)
                    .withDownstreamDependencies(this.getDownstreamDependencies(namespace));
            }
        } catch (Exception e) {
            System.out.println(e);
        } finally {
            try {
                if (stmt != null) {
                    stmt.close();
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

    public DatasetDescriptor registerDataset(DatasetDescriptor descriptor) {
        // NOTE: create entry in @datasets table and annotate dataset parameter
        UUID key = UUID.randomUUID();
        descriptor.withKey(key.toString());
        this.insertDataset(descriptor);
        // NOTE: connecting namespace with upstream
        for (String upstream : descriptor.getUpstreamDependencies()) {
            this.insertDependency(upstream, descriptor.getNamespace());
        }
        return descriptor;
    }

    public String[] getDownstreamDependencies(String namespace) {
        PreparedStatement stmt = null;
        try {
            stmt = this.conn.prepareStatement(FIND_DOWNSTREAM_DEPENDENCIES);
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
        } finally {
            try {
                if (stmt != null) {
                    stmt.close();
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        return null;
    }

    public void insertDataset(DatasetDescriptor dataset) {
        PreparedStatement stmt = null;
        try {
            stmt = this.conn.prepareStatement(INSERT_DATASET);
            stmt.setString(0, dataset.getNamespace());
            stmt.setString(1, dataset.getKey());
            stmt.setInt(2, dataset.getLayer());
            stmt.setString(3, String.join(",", dataset.getHeader().getNames()));
            stmt.execute();
        } catch (Exception e) {
            System.out.println(e);
        } finally {
            try {
                if (stmt != null) {
                    stmt.close();
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

    public void insertDependency(String namespaceSource, String namespaceTarget) {
        PreparedStatement stmt = null;
        try {
            stmt = this.conn.prepareStatement(INSERT_DEPENDENCY);
            stmt.setString(0, namespaceSource);
            stmt.setString(1, namespaceTarget);
            stmt.execute();
        } catch (Exception e) {
            System.out.println(e);
        } finally {
            try {
                if (stmt != null) {
                    stmt.close();
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

    public void storeDataset(Dataset dataset) {
        this.inserMultiple(dataset.getDescriptor().getKey(), dataset.getDescriptor().getHeader(), dataset.getBody());
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
        } finally {
            try {
                if (ps != null) {
                    ps.close();
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

    public Pair<String, List<String>> getRandomFeatures(int _layer, int rand) {
        PreparedStatement stmt = null;
        try {
            stmt = this.conn.prepareStatement(FIND_BY_LAYER);
            stmt.setInt(0, _layer);
            ResultSet rs = stmt.executeQuery();
            while (rs.next()) {
                String namespace = rs.getString("namespace");
                String[] features = rs.getString("features").split(",");
                List<String> selectedFeatures = (List<String>) RandomManager.getMany(features, rand);
                Pair<String, List<String>> ret = new Pair<>(namespace, selectedFeatures);
                return ret;
            }
            stmt.close();
        } catch (Exception e) {
            System.out.println(e);
        } finally {
            try {
                if (stmt != null) {
                    stmt.close();
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        return null;
    }

    public Body getDatasetBody(String table, String[] cols, int maxCount) {
        PreparedStatement stmt = null;
        try {
            String sql = String.format(FETCH_DATASET_BODY, table, maxCount);
            stmt = this.conn.prepareStatement(sql);
            ResultSet rs = stmt.executeQuery();
            Row[] rows = new Row[rs.getFetchSize()];
            int i = 0;
            while (rs.next()) {
                int j = 0;
                String[] vals = new String[cols.length];
                for (String col : cols) {
                    String val = rs.getString(col);
                    vals[j] = val;
                    j++;
                }
                rows[i] = new Row().withValues(vals);
                i++;
            }
            return new Body().withRows(rows);
        } catch (Exception e) {
            System.out.println(e);
        } finally {
            try {
                if (stmt != null) {
                    stmt.close();
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        return null;
    }

    public boolean tableExist(Connection conn, String tableName) {
        boolean tExists = false;
        try {
            try (ResultSet rs = conn.getMetaData().getTables(null, null, tableName, null)) {
                while (true) {
                    if (!rs.next()) break;
                    String tName = rs.getString("TABLE_NAME");
                    if (tName != null && tName.equals(tableName)) {
                        tExists = true;
                        break;
                    }
                }
            }
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }
        return tExists;
    }

    public boolean isInstalled() {
        if (this.isInstalled != null && this.isInstalled == true) {
            return true;
        }
        boolean installed = true;
        installed = installed && this.tableExist(this.conn, "@datasets");
        installed = installed && this.tableExist(this.conn, "@dependencies");
        return installed;
    }

    public void install() {
        if (this.isInstalled()) {
            return;
        }
        PreparedStatement stmt = null;
        try {
            Path path = Paths.get(String.format("%s/pg_install.sql", Config.DATA_DIR));
            String sql = new String(Files.readAllBytes(path));
            stmt = this.conn.prepareStatement(sql);
            stmt.executeUpdate();
            this.isInstalled = true;
        } catch(Exception e) {
            e.printStackTrace();
        } finally {
            try {
                if (stmt != null) {
                    stmt.close();
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

    public void reset() {
        try {
            this.conn.createStatement().execute(String.format("drop owned by %s", Config.DB_USER));
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
}
