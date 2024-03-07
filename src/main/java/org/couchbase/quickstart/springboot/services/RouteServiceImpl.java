package org.couchbase.quickstart.springboot.services;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import org.couchbase.quickstart.springboot.models.Route;
import org.couchbase.quickstart.springboot.repositories.RouteRepository;
import org.springframework.stereotype.Service;

@Service
public class RouteServiceImpl implements RouteService {

    private final RouteRepository routeRepository;

    public RouteServiceImpl(RouteRepository routeRepository) {
        this.routeRepository = routeRepository;
    }

    public Route getRouteById(String id) {
        return routeRepository.findById(id);
    }

    public Route createRoute(Route route) {
        return routeRepository.save(route);
    }

    public Route updateRoute(String id, Route route) {
        return routeRepository.update(id, route);
    }

    public void deleteRoute(String id) {
        routeRepository.delete(id);
    }

    public List<Route> listRoutes(int limit, int offset) {
        return routeRepository.findAll(limit, offset);
    }

    @Override
    public void createRoutes(int limit, int offset) {
        List<Route> routes = new ArrayList<>();
        routes = routeRepository.findAll(limit, offset);
        for (Route r : routes){
            System.out.println(r.getId());

            for (int i=0; i<=49; i++){
                UUID uuid = UUID.randomUUID();
                String doc_id = r.getId() +":"+ uuid.toString();
                //Convert
                byte[] route_byte = null;
                try (ByteArrayOutputStream bos = new ByteArrayOutputStream();
                     ObjectOutputStream oos = new ObjectOutputStream(bos)) {
                    oos.writeObject(r);
                    route_byte = bos.toByteArray();

                    routeRepository.save_byte(doc_id, route_byte);

                    // convert byte array to json and save

                    try (ByteArrayInputStream bis = new ByteArrayInputStream(route_byte);
                         ObjectInputStream ois = new ObjectInputStream(bis)) {
                        Object route =  ois.readObject();
                        routeRepository.save_json(doc_id, (Route) route);
                    } catch (IOException | ClassNotFoundException e) {
                        e.printStackTrace();
                    }

                } catch (IOException e) {
                    e.printStackTrace();
                }
            }


        }

    }

}
