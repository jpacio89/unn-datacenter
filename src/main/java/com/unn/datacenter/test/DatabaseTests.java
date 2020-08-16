package com.unn.datacenter.test;

import com.google.gson.Gson;
import com.unn.datacenter.models.*;
import com.unn.datacenter.service.DataService;
import org.junit.Test;
import spark.utils.Assert;
import java.util.UUID;

public class DatabaseTests {
    @Test
    public void testRegisterAgent() {
        DataService service = new DataService();
        service.init();
        String[] features = { "feature_a", "feature_b", "feature_c" };
        DatasetDescriptor descriptor = new DatasetDescriptor()
            .withLayer(0)
            .withNamespace("org.cortex.vision")
            .withHeader(new Header().withNames(features));
        service.registerAgent(descriptor);
    }

    @Test
    public void testStoreData() {
        DataService service = new DataService();
        service.init();
        String[] features = { "feature_a", "feature_b", "feature_c" };
        DatasetDescriptor descriptor = new DatasetDescriptor()
                .withLayer(0)
                .withNamespace("org.cortex.vision")
                .withHeader(new Header().withNames(features));
        Dataset dataset = new Dataset();
        dataset.withDescriptor(descriptor);
        String[] vals = {"1", "2", "3"};
        Row row = new Row().withValues(vals);
        Row[] rows = { row, row };
        dataset.withBody(new Body().withRows(rows));
        service.saveDataset(dataset);
    }
}
