package com.unn.datacenter.service;

import com.unn.datacenter.models.*;

public class CSVHelper {
    final String SEPARATOR = ",";

    public CSVHelper() { }

    public String toString(Dataset dataset) {
        StringBuilder builder = new StringBuilder();
        builder.append(String.join(SEPARATOR, dataset.getDescriptor().getHeader().getNames()));
        for (Row row : dataset.getBody().getRows()) {
            builder.append("\n");
            builder.append(String.join(SEPARATOR, row.getValues()));
        }
        return builder.toString();
    }

    public Dataset parse (String csv) {
        String[] lines = csv.split("\n");
        Row[] rows = new Row[lines.length-1];
        for (int i = 1; i < lines.length; ++i) {
            String[] data = lines[i].split(SEPARATOR);
            rows[i-1].withValues(data);
        }
        Dataset dataset = new Dataset()
            .withDescriptor(new DatasetDescriptor()
                    .withHeader(new Header()
                            .withNames(lines[0].split(SEPARATOR))))
            .withBody(new Body().withRows(rows));
        return dataset;
    }
}
