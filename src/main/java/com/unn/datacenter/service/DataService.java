package com.unn.datacenter.service;

import com.unn.datacenter.models.Dataset;

// TODO: servide that will save stuff in the storage and also notify listeners when new data is available
public class DataService {

    public DataService() {

    }

    public void saveDataset(Dataset dataset) {
        // TODO: read dataset descriptor from table '@datasets'
        // TODO: create dataset table if it doesnt exist
        // TODO: store dataset
        // TODO: identify uniques and primary sequential key if not sent by user
        // TODO: descriptor identifies relations between dataset and other datasets...
            // TODO: ...so that adding rows in another dataset trigger notifictions to be sent?
    }

    public void saveRow() {

    }

}
