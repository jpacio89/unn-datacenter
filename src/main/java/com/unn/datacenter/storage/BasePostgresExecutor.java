package com.unn.datacenter.storage;

import com.unn.common.dataset.*;
import com.unn.datacenter.Config;
import org.postgresql.Driver;

import javax.xml.crypto.Data;
import java.sql.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class BasePostgresExecutor implements DriverAction {
    protected Driver driver;
    protected Connection conn;

    protected Dataset datasetByResultSet(ResultSet resultSet) throws SQLException {
        Dataset dataset = new Dataset();
        String[] cols = new String[resultSet.getMetaData().getColumnCount()];
        for (int i = 0; i < cols.length; ++i) {
            cols[i] = resultSet.getMetaData().getColumnName(i + 1);
        }
        dataset.withDescriptor(new DatasetDescriptor()
            .withHeader(new Header().withNames(cols)));
        ArrayList<Row> rows = new ArrayList<>();
        while (resultSet.next()) {
            Row row = new Row()
                .withValues(Arrays.stream(cols).map(col -> {
                    try {
                        return resultSet.getString(col);
                    } catch (SQLException throwables) {
                        throwables.printStackTrace();
                        return null;
                    }
                }).toArray(String[]::new));
            rows.add(row);
        }
        dataset.withBody(new Body()
            .withRows(rows.stream().toArray(Row[]::new)));
        return dataset;
    }

    protected void execute(String sql, PostgresExecutor.IStatement stmtRun, PostgresExecutor.IResult resultRun, boolean isQuery) {
        PreparedStatement stmt = null;
        try {
            stmt = getConnection().prepareStatement(sql);
            if (stmtRun != null) {
                stmtRun.run(stmt);
            }
            if (isQuery) {
                ResultSet rs = stmt.executeQuery();
                if (resultRun != null) {
                    resultRun.run(rs);
                }
                rs.close();
            } else {
                stmt.execute();
            }
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

    public void init() {
        try {
            this.driver = new org.postgresql.Driver();
            DriverManager.registerDriver(driver, this);
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }

    protected Connection getConnection() {
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

    protected void closeResources() {
        try {
            if (this.conn != null && !this.conn.isClosed()) {
                //this.conn.close();
            }
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }
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

    protected String normalizeTableName(String table) {
        return table
                .replace(".", "_");
    }

    protected String normalizeColumnName(String feature) {
        return feature
                .replace("-", "_")
                .replace("\"", "")
                .replace(".", "c");
    }

    protected List<String> normalizeColumnNames(List<String> features) {
        return features.stream()
                .map(feature -> normalizeColumnName(feature))
                .collect(Collectors.toCollection(ArrayList::new));
    }

    protected List<String> normalizeColumnNames(String[] features) {
        return Arrays.stream(features)
                .map(feature -> normalizeColumnName(feature))
                .collect(Collectors.toCollection(ArrayList::new));
    }

    protected ArrayList<String> getFixedColumns(String[] features) {
        boolean hasPrimer = Arrays.stream(features).anyMatch("primer"::equals);
        ArrayList<String> fixedCols = new ArrayList<>();
        fixedCols.add("id integer");
        if (!hasPrimer) {
            fixedCols.add("primer integer");
        }
        return fixedCols;
    }

    protected ArrayList<String> getVariableColumns(String[] features) {
        return Arrays.stream(features)
                .map(feature -> feature.equals("primer") ?
                        String.format("%s integer", feature) :
                        String.format("%s character varying(64)", normalizeColumnName(feature)))
                .collect(Collectors.toCollection(ArrayList<String>::new));
    }
}
