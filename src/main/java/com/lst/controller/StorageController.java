package com.lst.controller;


import com.lst.model.GetRequest;
import com.lst.model.PutRequest;
import com.lst.storage.InMemoryStorage;
import com.lst.storage.Storage;
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


}
