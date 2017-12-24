package net.sourcedestination.mysql.db;

import net.sourcedestination.sai.db.DBInterface;
import net.sourcedestination.sai.graph.Feature;
import net.sourcedestination.sai.graph.Graph;
import net.sourcedestination.sai.graph.GraphFactory;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import java.util.stream.Stream;

/**
 * Created by jmorwick on 12/23/17.
 */
public class MysqlDBInterface implements DBInterface {

    @Override
    public void disconnect() {
        throw new NotImplementedException();
    }

    @Override
    public boolean isConnected() {
        throw new NotImplementedException();
    }

    @Override
    public <G extends Graph> G retrieveGraph(int graphID, GraphFactory<G> f) {
        throw new NotImplementedException();
    }

    @Override
    public Stream<Integer> getGraphIDStream() {
        throw new NotImplementedException();
    }

    @Override
    public Stream<Integer> retrieveGraphsWithFeature(Feature f) {
        throw new NotImplementedException();
    }

    @Override
    public Stream<Integer> retrieveGraphsWithFeatureName(String name) {
        throw new NotImplementedException();
    }

    @Override
    public void deleteGraph(int graphID) {
        throw new NotImplementedException();
    }

    @Override
    public int addGraph(Graph g) {
        throw new NotImplementedException();
    }

    @Override
    public int getDatabaseSize() {
        throw new NotImplementedException();
    }
}
