package org.couchbase.quickstart.springboot.repositories;

import java.util.List;

import com.couchbase.client.java.codec.RawBinaryTranscoder;
import com.couchbase.client.java.kv.UpsertOptions;
import org.couchbase.quickstart.springboot.configs.CouchbaseConfig;
import org.couchbase.quickstart.springboot.models.Route;
import org.springframework.stereotype.Repository;

import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.Collection;
import com.couchbase.client.java.json.JsonArray;
import com.couchbase.client.java.query.QueryOptions;
import com.couchbase.client.java.query.QueryScanConsistency;

@Repository
public class RouteRepositoryImpl implements RouteRepository {

    private final Cluster cluster;
    private final Collection routeCol;
    private final Collection routeCol_json;
    private final Collection routeCol_byte;
    private final CouchbaseConfig couchbaseConfig;

    public RouteRepositoryImpl(Cluster cluster, Bucket bucket, CouchbaseConfig couchbaseConfig) {
        this.cluster = cluster;
        this.routeCol = bucket.scope("inventory").collection("route");

        this.routeCol_byte = bucket.scope("inventory").collection("route_byte");
        this.routeCol_json = bucket.scope("inventory").collection("route_json");
        this.couchbaseConfig = couchbaseConfig;
    }

    @Override
    public Route findById(String id) {
        return routeCol.get(id).contentAs(Route.class);
    }

    @Override
    public Route save(Route route) {
        routeCol.insert(route.getId(), route);
        return route;
    }

    @Override
    public void save_json(String id, Route route) {
        System.out.println("Byte to Json: "+id);
        route.setId(id);
        routeCol_json.upsert(id, route);
    }

    @Override
    public void save_byte(String id, byte[] route_byte) {

        routeCol_byte.upsert(id, route_byte, UpsertOptions.upsertOptions().transcoder(RawBinaryTranscoder.INSTANCE));


    }
    @Override
    public Route update(String id, Route route) {
        routeCol.replace(id, route);
        return route;
    }

    @Override
    public void delete(String id) {
        routeCol.remove(id);
    }

    @Override
    public List<Route> findAll(int limit, int offset) {
        String statement = "SELECT route.* FROM `" + couchbaseConfig.getBucketName()
                + "`.`inventory`.`route` LIMIT $1 OFFSET $2";
        return cluster
                .query(statement,
                        QueryOptions.queryOptions().parameters(JsonArray.from(limit, offset))
                                .scanConsistency(QueryScanConsistency.REQUEST_PLUS))
                .rowsAs(Route.class);
    }
}
