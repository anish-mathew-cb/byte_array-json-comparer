package org.couchbase.quickstart.springboot.repositories;

import java.util.List;

import org.couchbase.quickstart.springboot.models.Route;

public interface RouteRepository {
    Route findById(String id);

    Route save(Route route);

    void save_json(String id, Route route);

    void save_byte(String id, byte[] route_byte);


    Route update(String id, Route route);

    void delete(String id);

    List<Route> findAll(int limit, int offset);

}