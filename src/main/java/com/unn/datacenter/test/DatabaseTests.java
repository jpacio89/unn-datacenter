package com.unn.datacenter.test;

import com.google.gson.Gson;
import com.unn.datacenter.models.*;
import com.unn.datacenter.service.DataService;
import org.junit.Test;
import spark.utils.Assert;

import java.util.HashMap;
import java.util.List;
import java.util.UUID;

public class DatabaseTests {

    DatasetDescriptor getDescriptor() {
        String[] features = { "feature_a", "feature_b", "feature_c" };
        return new DatasetDescriptor()
            .withLayer(0)
            .withNamespace("org.cortex.vision")
            .withHeader(new Header().withNames(features));
    }

    Body getBody() {
        String[] vals = {"1", "2", "3"};
        Row row = new Row().withValues(vals);
        Row[] rows = { row, row };
        return new Body().withRows(rows);
    }

    @Test
    public void testRegisterAgent() {
        DataService service = new DataService();
        service.init();
        DatasetDescriptor descriptor = getDescriptor();
        service.registerAgent(descriptor);
    }

    @Test
    public void testStoreData() {
        DataService service = new DataService();
        service.init();
        DatasetDescriptor descriptor = getDescriptor();
        Body body = getBody();
        Dataset dataset = new Dataset()
            .withDescriptor(descriptor)
            .withBody(body);
        service.saveDataset(dataset);
    }

    @Test
    public void testRandomFeatures() {
        DataService service = new DataService();
        service.init();
        HashMap<String, List<String>> ret = service.getRandomFeatures(0, 10);
        System.out.println(ret);
    }
}
