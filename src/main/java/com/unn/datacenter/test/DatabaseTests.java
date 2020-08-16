package com.unn.datacenter.test;

import com.google.gson.Gson;
import com.unn.datacenter.models.DatasetDescriptor;
import com.unn.datacenter.models.Header;
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
}
