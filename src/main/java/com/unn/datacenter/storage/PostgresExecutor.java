package com.unn.datacenter.storage;

import com.unn.common.dataset.*;
import com.unn.datacenter.Config;
import com.unn.common.utils.RandomManager;
import javafx.util.Pair;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.*;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

public class PostgresExecutor extends BasePostgresExecutor {
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
    final String FETCH_TIMED_DATASET = "select * from %s where primer > ? order by primer asc limit %d offset 0";
    Boolean isInstalled;

    public PostgresExecutor() { }

    public void createNamespace(String namespace, String[] features) {
        boolean hasPrimer = Arrays.stream(features).anyMatch("primer"::equals);
        String table = normalizeTableName(namespace);
        ArrayList<String> fixedColumns = getFixedColumns(features);
        ArrayList<String> variableColumns = getVariableColumns(features);
        String fixedColumnsSql = String.join(",", fixedColumns);
        String variableColumnsSql = String.join(",", variableColumns);

        StringBuilder builder = new StringBuilder()
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
        execute(sql, null, null, false);
    }

    public void annotateDataset(DatasetDescriptor descriptor) {
        String namespace = descriptor.getNamespace();
        execute(FIND_BY_NAMESPACE,
            (statement) -> statement.setString(1, namespace),
            (resultSet) -> {
                while (resultSet.next()) {
                    String key = resultSet.getString("key");
                    int layer = resultSet.getInt("layer");
                    descriptor
                        .withKey(key)
                        .withLayer(layer)
                        .withUpstreamDependencies(this.getUpstreamDependencies(namespace));
                }
            }, true);
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
        ArrayList<String> depends = new ArrayList<>();
        execute(FIND_UPSTREAM_DEPENDENCIES,
            (statement) -> statement.setString(1, namespace),
            (resultSet) -> {
                while (resultSet.next()) {
                    depends.add(resultSet.getString("upstream"));
                }
            }, true);
        return depends.toArray(new String[depends.size()]);
    }

    public void insertDataset(DatasetDescriptor dataset) {
        execute(INSERT_DATASET,
            (statement) -> {
                String features = "";
                if (dataset.getHeader().getNames() != null) {
                    features = String.join(",", dataset.getHeader().getNames());
                }
                statement.setString(1, dataset.getNamespace());
                statement.setString(2, dataset.getKey());
                statement.setInt(3, dataset.getLayer());
                statement.setString(4, features);
            }, null, false);
    }

    public void insertDependency(String namespaceSource, String namespaceTarget) {
        execute(INSERT_DEPENDENCY,
            (statement) -> {
                statement.setString(1, namespaceSource);
                statement.setString(2, namespaceTarget);
            }, null, false);
    }

    public void storeDataset(Dataset dataset) {
        this.install();
        String namespace = dataset.getDescriptor().getNamespace();
        String table = normalizeTableName(namespace);
        Header header = dataset.getDescriptor().getHeader();
        Body body = dataset.getBody();
        this.inserMultiple(table, header, body);
    }

    public Pair<String, List<String>> getRandomFeatures(int _layer, int rand) {
        AtomicReference<Pair<String, List<String>>> ret = null;
        execute(FIND_BY_LAYER,
            (statement) -> statement.setInt(1, _layer),
            (resultSet) -> {
                while (resultSet.next()) {
                    String namespace = resultSet.getString("namespace");
                    String[] features = resultSet.getString("features").split(",");
                    List<String> selectedFeatures = (List<String>) RandomManager.getMany(features, rand);
                    ret.set(new Pair<>(namespace, selectedFeatures));
                    break;
                }
            }, true);
        return ret.get();
    }

    public HashMap<String, ArrayList<String>> getDatasetBody(String namespace, List<String> features,
        int maxCount, ArrayList<String> whitelistTimes, ArrayList<Integer> blacklistTimes) {
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
        HashMap<String, ArrayList<String>> dataset = new HashMap<>();

        execute(sql, null,
            (resultSet) -> {
                String[] cols = new String[resultSet.getMetaData().getColumnCount()];

                for (int i = 0; i < cols.length; ++i) {
                    cols[i] = resultSet.getMetaData().getColumnName(i+1);
                }

                while (resultSet.next()) {
                    ArrayList<String> vals = new ArrayList<>();
                    for (String col : cols) {
                        String val = resultSet.getString(col);
                        vals.add(val);
                    }
                    dataset.put(vals.get(1), vals);
                }
            }, true
        );

        return dataset;
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

        execute(sql, null,
            (resultSet) -> {
                while (resultSet.next()) {
                    String id = resultSet.getString("primer");
                    ids.add(id);
                }
            }, true);

        return ids;
    }

    public ArrayList<Integer> getNamespaceMakerPrimers(String namespace) {
        ArrayList<Integer> primers = new ArrayList<>();
        execute(FIND_MAKER_PRIMERS_BY_NAMESPACE,
            (statement) -> statement.setString(1, namespace),
            (resultSet) -> {
                while (resultSet.next()) {
                    Integer primer = resultSet.getInt("primer");
                    primers.add(primer);
                }
            }, true);
        return primers;
    }

    public ArrayList<String> getNamespaces() {
        ArrayList<String> namespaces = new ArrayList<>();
        execute(FIND_NAMESPACES, null, (resultSet) -> {
                while (resultSet.next()) {
                    String primer = resultSet.getString("namespace");
                    namespaces.add(primer);
                }
            }, true);
        return namespaces;
    }

    private void inserMultiple(String table, Header header, Body body) {
        final int batchSize = 10;
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

    public Dataset getDatasetByTime(String namespace, int fromPrimer) {
        AtomicReference<Dataset> dataset = new AtomicReference<>(new Dataset());
        execute(String.format(FETCH_TIMED_DATASET, normalizeTableName(namespace), 1000),
            (stmt -> stmt.setInt(1, fromPrimer)),
            (resultSet) -> dataset.set(datasetByResultSet(namespace, resultSet)), true
        );
        return dataset.get();
    }

    public interface IStatement {
        void run (PreparedStatement stmt) throws SQLException;
    }

    public interface IResult {
        void run (ResultSet rs) throws SQLException;
    }
}
