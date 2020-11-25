package com.unn.datacenter.storage;

import com.unn.common.dataset.*;
import com.unn.datacenter.Config;
import com.unn.common.utils.RandomManager;
import javafx.util.Pair;
import org.postgresql.Driver;

import java.io.IOException;
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
    final String FIND_MISSING_TIMES = "select primer from %s where primer > (case when (select max(primer) as primer from %s) is null then 0 else (select max(primer) as primer from %s) end) %s limit 1000";
    final String INSERT_MAKER_PRIMERS = "insert into \"@maker_primers\" (namespace, primer) values (?, ?)";
    final String FIND_MAKER_PRIMERS_BY_NAMESPACE = "select primer from \"@maker_primers\" where namespace = ?";
    final String FIND_NAMESPACES = "select * from \"@datasets\"";
    Driver driver;
    Connection conn;
    Boolean isInstalled;

    public PostgresExecutor() {
    }

    public void init() {
        try {
            this.driver = new org.postgresql.Driver();
            DriverManager.registerDriver(driver, this);
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }

    private Connection getConnection() {
        try {
            if (this.conn != null) {
                return this.conn;
            }
            if (this.conn != null && !this.conn.isClosed()) {
                //this.conn.close();
            }
            String connectionPath = String.format(
                "jdbc:postgresql://%s:%s/%s", Config.DB_HOST, Config.DB_PORT, Config.DB_NAME);
            this.conn = DriverManager.getConnection(connectionPath, Config.DB_USER, Config.DB_PASSWORD);
        } catch (SQLException throwables) {
            throwables.printStackTrace();
            return null;
        }
        return this.conn;
    }

    private void closeResources() {
        try {
            if (this.conn != null && !this.conn.isClosed()) {
                //this.conn.close();
            }
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }
    }

    public void createNamespace(String namespace, String[] features) {
        PreparedStatement stmt = null;
        StringBuilder builder = null;
        try {
            boolean hasPrimer = Arrays.stream(features).anyMatch("primer"::equals);
            String table = normalizeTableName(namespace);
            ArrayList<String> fixedColumns = getFixedColumns(features);
            ArrayList<String> variableColumns = getVariableColumns(features);
            String fixedColumnsSql = String.join(",", fixedColumns);
            String variableColumnsSql = String.join(",", variableColumns);
            builder = new StringBuilder()
                .append("DROP TABLE IF EXISTS **TABLE**;\n")
                .append(String.format("CREATE TABLE **TABLE** (%s,%s);\n", fixedColumnsSql, variableColumnsSql))
                .append("CREATE SEQUENCE **TABLE**_id_seq START WITH 1 INCREMENT BY 1 NO MINVALUE NO MAXVALUE CACHE 1;\n")
                .append("ALTER SEQUENCE **TABLE**_id_seq OWNED BY **TABLE**.id;\n")
                .append("ALTER TABLE ONLY **TABLE** ALTER COLUMN id SET DEFAULT nextval('**TABLE**_id_seq'::regclass);\n")
                .append("GRANT ALL PRIVILEGES ON TABLE **TABLE** TO rabbitpt;\n")
                .append("CREATE INDEX primer_**TABLE**_index ON **TABLE** USING BTREE (primer);\n");

            if (!hasPrimer) {
                builder.append("CREATE SEQUENCE **TABLE**_primer_seq START WITH 1 INCREMENT BY 1 NO MINVALUE NO MAXVALUE CACHE 1;\n")
                .append("ALTER SEQUENCE **TABLE**_primer_seq OWNED BY **TABLE**.primer;\n")
                .append("ALTER TABLE ONLY **TABLE** ALTER COLUMN primer SET DEFAULT nextval('**TABLE**_primer_seq'::regclass);\n");
            }

            String sql = builder.toString().replace("**TABLE**", table);
            stmt = getConnection().prepareStatement(sql);
            stmt.execute();
        } catch (Exception e) {
            System.err.println(builder.toString());
            e.printStackTrace();
        } finally {
            try {
                if (stmt != null) {
                    stmt.close();
                }
                closeResources();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

    public void annotateDataset(DatasetDescriptor descriptor) {
        PreparedStatement stmt = null;
        try {
            String namespace = descriptor.getNamespace();
            stmt = getConnection().prepareStatement(FIND_BY_NAMESPACE);
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
            rs.close();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                if (stmt != null) {
                    stmt.close();
                }
                closeResources();
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
            stmt = getConnection().prepareStatement(FIND_UPSTREAM_DEPENDENCIES);
            stmt.setString(1, namespace);
            ResultSet rs = stmt.executeQuery();
            ArrayList<String> depends = new ArrayList<>();
            while (rs.next()) {
                depends.add(rs.getString("upstream"));
            }
            rs.close();
            return depends.toArray(new String[depends.size()]);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                if (stmt != null) {
                    stmt.close();
                }
                closeResources();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        return null;
    }

    public String[] getDownstreamDependencies(String namespace) {
        PreparedStatement stmt = null;
        try {
            stmt = getConnection().prepareStatement(FIND_DOWNSTREAM_DEPENDENCIES);
            stmt.setString(1, namespace);
            ResultSet rs = stmt.executeQuery();
            int size = rs.getFetchSize();
            String[] depends = new String[size];
            int i = 0;
            while (rs.next()) {
                depends[i] = rs.getString("downstream");
                ++i;
            }
            rs.close();
            return depends;
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                if (stmt != null) {
                    stmt.close();
                }
                closeResources();
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
            stmt = getConnection().prepareStatement(INSERT_DATASET);
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
                closeResources();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

    public void insertDependency(String namespaceSource, String namespaceTarget) {
        PreparedStatement stmt = null;
        try {
            stmt = getConnection().prepareStatement(INSERT_DEPENDENCY);
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
                closeResources();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

    public void storeDataset(Dataset dataset) {
        this.install();
        String namespace = dataset.getDescriptor().getNamespace();
        String table = normalizeTableName(namespace);
        Header header = dataset.getDescriptor().getHeader();
        Body body = dataset.getBody();
        this.inserMultiple(table, header, body);
    }

    private void inserMultiple(String table, Header header, Body body) {
        final int batchSize = 1000;
        PreparedStatement ps = null;
        try {
            String[] features = header.getNames();
            String[] template = new String[header.getNames().length];
            Arrays.fill(template, "?");
            String sql = String.format(
                "INSERT INTO %s (%s) VALUES (%s)",
                table,
                String.join(",", normalizeColumnNames(header.getNames())),
                String.join(",", template)
            );
            ps = getConnection().prepareStatement(sql);
            int insertCount = 0;
            for (Row row : body.getRows()) {
                String[] values = row.getValues();
                for (int j = 0; j < values.length; ++j) {
                    String feature = features[j];
                    if (feature.equals("primer")) {
                        ps.setInt(j+1, Integer.parseInt(values[j]));
                    } else {
                        ps.setString(j+1, values[j]);
                    }
                }
                ps.addBatch();
                if (++insertCount % batchSize == 0) {
                    ps.executeBatch();
                }
            }
            ps.executeBatch();
        } catch (SQLException e) {
            SQLException sqlException = e.getNextException();
            e.printStackTrace();
            sqlException.printStackTrace();
        } finally {
            try {
                if (ps != null) {
                    ps.close();
                }
                closeResources();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

    public Pair<String, List<String>> getRandomFeatures(int _layer, int rand) {
        PreparedStatement stmt = null;
        try {
            stmt = getConnection().prepareStatement(FIND_BY_LAYER);
            stmt.setInt(1, _layer);
            ResultSet rs = stmt.executeQuery();
            while (rs.next()) {
                String namespace = rs.getString("namespace");
                String[] features = rs.getString("features").split(",");
                List<String> selectedFeatures = (List<String>) RandomManager.getMany(features, rand);
                Pair<String, List<String>> ret = new Pair<>(namespace, selectedFeatures);
                return ret;
            }
            rs.close();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                if (stmt != null) {
                    stmt.close();
                }
                closeResources();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        return null;
    }

    public HashMap<String, ArrayList<String>> getDatasetBody(String namespace, List<String> features,
        int maxCount, ArrayList<String> whitelistTimes, ArrayList<Integer> blacklistTimes) {
        PreparedStatement stmt = null;
        try {
            String table = normalizeTableName(namespace);
            String columnNames = features == null ? "*" : "id,primer," + String.join(",", features);
            StringBuilder timesWhere = new StringBuilder();

            if (whitelistTimes != null) {
                if (whitelistTimes.size() == 0) {
                    whitelistTimes.add("0");
                }
                timesWhere.append(String.format("where primer in (%s)",
                    String.join(",", whitelistTimes)));
            }

            if (blacklistTimes != null && blacklistTimes.size() > 0) {
                timesWhere.append(String.format("%s primer not in (%s)",
                    timesWhere.length() == 0 ? "where" : "and",
                    String.join(",", blacklistTimes.stream()
                        .map(Object::toString)
                        .collect(Collectors.toCollection(ArrayList::new)))));
            }

            String sql = String.format(FETCH_DATASET_BODY, columnNames, table, timesWhere.toString(), maxCount);
            stmt = getConnection().prepareStatement(sql);
            ResultSet rs = stmt.executeQuery();
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
                dataset.put(vals.get(1), vals);
            }

            rs.close();
            return dataset;
        } catch (SQLException e) {
            e.getNextException().printStackTrace();
            e.printStackTrace();
        } finally {
            try {
                if (stmt != null) {
                    stmt.close();
                }
                closeResources();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        return null;
    }

    public ArrayList<String> getMissingTimes(String namespace, String[] upstreamNamespaces) {
        StringBuilder restriction = new StringBuilder();

        for (String upstreamNamespace : upstreamNamespaces) {
            String upperTable = normalizeTableName(upstreamNamespace);
            restriction.append(String.format(" and primer in (select primer from %s)", upperTable));
        }

        String sql = String.format(FIND_MISSING_TIMES,
            normalizeTableName(upstreamNamespaces[0]),
            normalizeTableName(namespace),
            normalizeTableName(namespace),
            restriction.toString());

        ArrayList<String> ids = new ArrayList<>();

        executeQuery(sql, null,
            (resultSet) -> {
                while (resultSet.next()) {
                    String id = resultSet.getString("primer");
                    ids.add(id);
                }
            }
        );

        return ids;
    }

    public void registerMakerPrimers(String namespace, Integer[] makerPrimers) {
        PreparedStatement stmt = null;
        try {
            stmt = getConnection().prepareStatement(INSERT_MAKER_PRIMERS);
            PreparedStatement finalStmt = stmt;
            Arrays.stream(makerPrimers).forEach(makerPrimer -> {
                try {
                    finalStmt.setString(1, namespace);
                    finalStmt.setInt(2, makerPrimer);
                    finalStmt.addBatch();
                } catch (SQLException throwables) {
                    throwables.printStackTrace();
                }
            });
            stmt.executeBatch();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                if (stmt != null) {
                    stmt.close();
                }
                closeResources();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

    public ArrayList<Integer> getNamespaceMakerPrimers(String namespace) {
        ArrayList<Integer> primers = new ArrayList<>();
        executeQuery(FIND_MAKER_PRIMERS_BY_NAMESPACE,
            (statement) -> statement.setString(1, namespace),
            (resultSet) -> {
                while (resultSet.next()) {
                    Integer primer = resultSet.getInt("primer");
                    primers.add(primer);
                }
            }
        );
        return primers;
    }

    public ArrayList<String> getNamespaces() {
        ArrayList<String> namespaces = new ArrayList<>();
        executeQuery(FIND_NAMESPACES, null, (resultSet) -> {
            while (resultSet.next()) {
                String primer = resultSet.getString("namespace");
                namespaces.add(primer);
            }
        });
        return namespaces;
    }

    public Set<String> getParentNamespaces(String namespace) {
        String[] dependencies = this.getUpstreamDependencies(namespace);
        return Arrays.stream(dependencies)
            .map(dependency -> dependency.split("@")[1])
            .collect(Collectors.toSet());
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
                rs.close();
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
        Connection conn = getConnection();
        installed = installed && this.tableExist(conn, "@datasets");
        installed = installed && this.tableExist(conn, "@dependencies");
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
            stmt = getConnection().prepareStatement(sql);
            stmt.execute();
            this.isInstalled = isInstalled();
        } catch(SQLException e) {
            e.getNextException().printStackTrace();
            e.printStackTrace();
        } catch (IOException e) {
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
            getConnection().createStatement().execute(String.format("drop owned by %s", Config.DB_USER));
        } catch (Exception e) {
            System.out.println(e);
        }
        this.isInstalled = false;
        this.install();
    }

    @Override
    public void deregister() {
        try {
            getConnection().close();
            DriverManager.deregisterDriver(this.driver);
        }
        catch (Exception e) {
            System.out.println(e);
        }
    }

    private void executeQuery(String sql, IStatement stmtRun, IResult resultRun) {
        PreparedStatement stmt = null;
        try {
            stmt = getConnection().prepareStatement(sql);
            if (stmtRun != null) {
                stmtRun.run(stmt);
            }
            ResultSet rs = stmt.executeQuery();
            if (resultRun != null) {
                resultRun.run(rs);
            }
            rs.close();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                if (stmt != null) {
                    stmt.close();
                }
                closeResources();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

    private String normalizeTableName(String table) {
        return table
            .replace(".", "_");
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

    private ArrayList<String> getFixedColumns(String[] features) {
        boolean hasPrimer = Arrays.stream(features).anyMatch("primer"::equals);
        ArrayList<String> fixedCols = new ArrayList<>();
        fixedCols.add("id integer");
        if (!hasPrimer) {
            fixedCols.add("primer integer");
        }
        return fixedCols;
    }

    private ArrayList<String> getVariableColumns(String[] features) {
        return Arrays.stream(features)
                .map(feature -> feature.equals("primer") ?
                    String.format("%s integer", feature) :
                    String.format("%s character varying(64)", normalizeColumnName(feature)))
                .collect(Collectors.toCollection(ArrayList<String>::new));
    }

    public interface IStatement {
        void run (PreparedStatement stmt) throws SQLException;
    }

    public interface IResult {
        void run (ResultSet rs) throws SQLException;
    }
}
