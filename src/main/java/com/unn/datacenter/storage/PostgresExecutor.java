package com.unn.datacenter.storage;

import com.unn.common.dataset.*;
import com.unn.datacenter.Config;
import com.unn.common.utils.RandomManager;
import javafx.util.Pair;
import org.postgresql.Driver;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.*;
import java.util.*;
import java.util.stream.Collectors;

public class PostgresExecutor implements DriverAction {
    final String FIND_BY_NAMESPACE = "select * from \"@datasets\" where namespace = ?";
    final String INSERT_DATASET = "insert into \"@datasets\" (namespace, key, layer, features) values (?, ?, ?, ?)";
    final String INSERT_DEPENDENCY = "insert into \"@dependencies\" (upstream, downstream) values (?, ?)";
    final String FIND_DOWNSTREAM_DEPENDENCIES = "select * from \"@dependencies\" where upstream = ?";
    final String FIND_UPSTREAM_DEPENDENCIES = "select * from \"@dependencies\" where downstream = ?";
    final String FIND_BY_LAYER = "select * from \"@datasets\" where layer = ? order by random() limit 1 offset 0";
    final String FETCH_DATASET_BODY = "select %s from %s %s order by random() limit %d offset 0";
    // TODO: replace id by primer
    final String FIND_MISSING_TIMES = "select id from %s where id not in (select CAST (primer AS INTEGER) from %s) %s";
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

    public void createTable(String namespace, String[] features) {
        PreparedStatement stmt = null;
        StringBuilder builder = null;
        try {
            String table = namespace.replace(".", "_");
            String[] fixedCols = { "id integer" };
            String[] cols =  new String[features.length];
            for (int i = 0; i < cols.length; ++i) {
                String name = String.format("%s", normalizeColumnName(features[i]));
                cols[i] = String.format("%s character varying(64)", name);
            }
            String fixedColsSql = String.join(",", fixedCols);
            String colsSql = String.join(",", cols);
            builder = new StringBuilder()
                .append(String.format("DROP TABLE IF EXISTS %s;", table))
                .append(String.format("CREATE TABLE  %s (%s,%s);", table, fixedColsSql, colsSql))
                .append(String.format("CREATE SEQUENCE %s_id_seq START WITH 1 INCREMENT BY 1 NO MINVALUE NO MAXVALUE CACHE 1;", table))
                .append(String.format("ALTER SEQUENCE %s_id_seq OWNED BY %s.id;\n", table, table))
                .append(String.format("ALTER TABLE ONLY %s ALTER COLUMN id SET DEFAULT nextval('%s_id_seq'::regclass);", table, table))
                .append(String.format("GRANT ALL PRIVILEGES ON TABLE %s TO rabbitpt;", table));
            stmt = this.conn.prepareStatement(builder.toString());
            stmt.execute();
        } catch (Exception e) {
            System.err.println(builder.toString());
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

    public void annotateDataset(DatasetDescriptor descriptor) {
        PreparedStatement stmt = null;
        try {
            String namespace = descriptor.getNamespace();
            stmt = this.conn.prepareStatement(FIND_BY_NAMESPACE);
            stmt.setString(1, namespace);
            ResultSet rs = stmt.executeQuery();
            while (rs.next()) {
                String key = rs.getString("key");
                int layer = rs.getInt("layer");
                descriptor
                    .withKey(key)
                    .withLayer(layer)
//                    .withDownstreamDependencies(this.getDownstreamDependencies(namespace))
                    .withUpstreamDependencies(this.getUpstreamDependencies(namespace));
            }
        } catch (Exception e) {
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

    public DatasetDescriptor registerDataset(DatasetDescriptor descriptor) {
        this.install();
        // NOTE: create entry in @datasets table and annotate dataset parameter
        UUID key = UUID.randomUUID();
        descriptor.withKey(key.toString());
        this.insertDataset(descriptor);
        // NOTE: connecting namespace with upstream
        if (descriptor.getUpstreamDependencies() != null) {
            for (String upstream : descriptor.getUpstreamDependencies()) {
                this.insertDependency(upstream, descriptor.getNamespace());
            }
        }
        return descriptor;
    }

    public String[] getUpstreamDependencies(String namespace) {
        PreparedStatement stmt = null;
        try {
            stmt = this.conn.prepareStatement(FIND_UPSTREAM_DEPENDENCIES);
            stmt.setString(1, namespace);
            ResultSet rs = stmt.executeQuery();
            ArrayList<String> depends = new ArrayList<>();
            while (rs.next()) {
                depends.add(rs.getString("upstream"));
            }
            return depends.toArray(new String[depends.size()]);
        } catch (Exception e) {
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
        return null;
    }

    public String[] getDownstreamDependencies(String namespace) {
        PreparedStatement stmt = null;
        try {
            stmt = this.conn.prepareStatement(FIND_DOWNSTREAM_DEPENDENCIES);
            stmt.setString(1, namespace);
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
        return null;
    }

    public void insertDataset(DatasetDescriptor dataset) {
        PreparedStatement stmt = null;
        String features = "";
        if (dataset.getHeader().getNames() != null) {
            features = String.join(",", dataset.getHeader().getNames());
        }
        try {
            stmt = this.conn.prepareStatement(INSERT_DATASET);
            stmt.setString(1, dataset.getNamespace());
            stmt.setString(2, dataset.getKey());
            stmt.setInt(3, dataset.getLayer());
            stmt.setString(4, features);
            stmt.execute();
        } catch (Exception e) {
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

    public void insertDependency(String namespaceSource, String namespaceTarget) {
        PreparedStatement stmt = null;
        try {
            stmt = this.conn.prepareStatement(INSERT_DEPENDENCY);
            stmt.setString(1, namespaceSource);
            stmt.setString(2, namespaceTarget);
            stmt.execute();
        } catch (Exception e) {
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

    public void storeDataset(Dataset dataset) {
        this.install();
        this.inserMultiple(dataset.getDescriptor().getNamespace()
            .replace(".", "_"),
            dataset.getDescriptor().getHeader(), dataset.getBody());
    }

    private void inserMultiple(String table, Header header, Body body) {
        final int batchSize = 1000;
        PreparedStatement ps = null;
        try {
            String[] template = new String[header.getNames().length];
            Arrays.fill(template, "?");
            String sql = String.format(
                "INSERT INTO %s (%s) VALUES (%s)",
                table,
                String.join(",", normalizeColumnNames(header.getNames())),
                String.join(",", template)
            );
            ps = this.conn.prepareStatement(sql);
            int insertCount = 0;
            for (Row row : body.getRows()) {
                String[] values = row.getValues();
                for (int j = 0; j < values.length; ++j) {
                    ps.setString(j+1, values[j]);
                }
                ps.addBatch();
                if (++insertCount % batchSize == 0) {
                    ps.executeBatch();
                }
            }
            ps.executeBatch();
        } catch (SQLException e) {
            e.printStackTrace();
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
            stmt.setInt(1, _layer);
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
        return null;
    }

    public HashMap<String, ArrayList<String>> getDatasetBody(String namespace, List<String> features, int maxCount, ArrayList<String> times) {
        PreparedStatement stmt = null;
        try {
            String table = namespace.replace(".", "_");
            String colNames = features == null ? "*" : "id," +
                String.join(",", normalizeColumnNames(features));
            String timesWhere = "";
            if (times != null) {
                timesWhere = String.format("where id in (%s)", String.join(",", times));
            }
            String sql = String.format(FETCH_DATASET_BODY, colNames, table, timesWhere, maxCount);
            stmt = this.conn.prepareStatement(sql);
            ResultSet rs = stmt.executeQuery();
            ArrayList<Row> rows = new ArrayList<>();
            String[] cols = new String[rs.getMetaData().getColumnCount()];
            for (int i = 0; i < cols.length; ++i) {
                cols[i] = rs.getMetaData().getColumnName(i+1);
            }
            HashMap<String, ArrayList<String>> dataset = new HashMap<>();
            while (rs.next()) {
                ArrayList<String> vals = new ArrayList<>();
                for (String col : cols) {
                    String val = rs.getString(col);
                    vals.add(val);
                }
                dataset.put(vals.get(0), vals);
            }
            return dataset;
        } catch (Exception e) {
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
        return null;
    }

    public ArrayList<String> getMissingTimes(String namespace, String[] upstreamNamespaces) {
        PreparedStatement stmt = null;
        try {
            StringBuilder restriction = new StringBuilder();
            for (String upstreamNamespace : upstreamNamespaces) {
                restriction.append(String.format(" and id in (select id from %s)", upstreamNamespace.replace(".", "_")));
            }
            String q = String.format(FIND_MISSING_TIMES,
                upstreamNamespaces[0].replace(".", "_"),
                namespace.replace(".", "_"),
                restriction.toString());
            stmt = this.conn.prepareStatement(q);
            ResultSet rs = stmt.executeQuery();
            ArrayList<String> ids = new ArrayList<>();
            while (rs.next()) {
                String id = rs.getString("id");
                ids.add(id);
            }
            return ids;
        } catch (Exception e) {
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
            this.isInstalled = isInstalled();
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
        this.isInstalled = false;
        this.install();
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

    private String normalizeColumnName(String feature) {
        return feature
            .replace("-", "_")
            .replace("\"", "")
            .replace(".", "c");
    }

    private List<String> normalizeColumnNames(List<String> features) {
        return features.stream()
            .map(feature -> normalizeColumnName(feature))
            .collect(Collectors.toCollection(ArrayList::new));
    }

    private List<String> normalizeColumnNames(String[] features) {
        return Arrays.stream(features)
            .map(feature -> normalizeColumnName(feature))
            .collect(Collectors.toCollection(ArrayList::new));
    }
}
