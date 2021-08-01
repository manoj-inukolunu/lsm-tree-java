package com.lsmt.controller;


import com.lsmt.model.GetRequest;
import com.lsmt.model.PutRequest;
import com.lsmt.storage.InMemoryStorage;
import com.lsmt.storage.Storage;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class StorageController {


    private final Storage storage;


    public StorageController(InMemoryStorage storage) {
        this.storage = storage;
    }

    @RequestMapping(value = "/getKey", method = RequestMethod.POST)
    public String get(@RequestBody GetRequest request) {
        return storage.get(request.getKey());
    }

    @RequestMapping(value = "/putKey", method = RequestMethod.POST)
    public void put(@RequestBody PutRequest putRequest) {
        storage.put(putRequest.getKey(), putRequest.getValue());
    }

    @RequestMapping(value = "/deleteKey", method = RequestMethod.DELETE)
    public void delete(@RequestBody GetRequest request) {
        storage.delete(request.getKey());
    }


}
