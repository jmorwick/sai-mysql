package net.sourcedestination.mysql.db;

import net.sourcedestination.sai.db.DBInterface;
import net.sourcedestination.sai.graph.*;

import java.sql.*;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * Created by jmorwick on 12/23/17.
 */
public class MysqlDBInterface implements DBInterface {

    private Logger logger = Logger.getLogger(MysqlDBInterface.class.getName());


    public static final String INIT_SQL =
                    "DROP TABLE IF EXISTS `graph_instances`;\n" +
                    "CREATE TABLE `graph_instances` (\n" +
                    "  `id` int(11) NOT NULL auto_increment,\n" +
                    "  `nodes` int(11) NOT NULL,\n" +
                    "  `edges` int(11) NOT NULL,\n" +
                    "  `features` int(11) NOT NULL COMMENT 'number of associated features',\n" +
                    "  PRIMARY KEY  (`id`)\n" +
                    ") ENGINE=MyISAM DEFAULT CHARSET=latin1 COMMENT='instances of graphs';\n" +
                    "\n" +
                    "DROP TABLE IF EXISTS `node_instances`;\n" +
                    "CREATE TABLE `node_instances` (\n" +
                    "  `id` int(11) NOT NULL COMMENT 'unique within a graph, not globally unique',\n" +
                    "  `graph_id` int(11) NOT NULL COMMENT 'foreign key (graph_instances->id)',\n" +
                    "  `features` int(11) NOT NULL COMMENT 'number of associated features',\n" +
                    "  KEY `id` (`id`,`graph_id`)\n" +
                    ") ENGINE=MyISAM DEFAULT CHARSET=latin1 COMMENT='instance of a node in a graph instance';\n" +
                    "\n" +
                    "DROP TABLE IF EXISTS `edge_instances`;\n" +
                    "CREATE TABLE `edge_instances` (\n" +
                    "  `id` int(11) NOT NULL COMMENT 'unique within a graph, not globally unique',\n" +
                    "  `graph_id` int(11) NOT NULL COMMENT 'foreign key (graph_instances->id)',\n" +
                    "  `from_node_id` int(11) NOT NULL COMMENT 'output node id (node_instances->id)',\n" +
                    "  `to_node_id` int(11) NOT NULL COMMENT 'input node id (node_instances->id)',\n" +
                    "  `features` int(11) NOT NULL COMMENT 'number of associated features',\n" +
                    "  KEY `id` (`id`,`graph_id`),\n" +
                    "  KEY `graph_id` (`graph_id`,`from_node_id`,`to_node_id`),\n" +
                    "  KEY `from_node_id` (`graph_id`,`from_node_id`),\n" +
                    "  KEY `to_node_id` (`graph_id`,`to_node_id`)\n" +
                    ") ENGINE=MyISAM DEFAULT CHARSET=latin1 COMMENT='edge between two nodes';\n" +
                    "\n" +
                    "DROP TABLE IF EXISTS `node_features`;\n" +
                    "CREATE TABLE `node_features` (\n" +
                    "  `graph_id` int(11) NOT NULL COMMENT 'tagged graph (graph_instances->id)',\n" +
                    "  `node_id` int(11) NOT NULL COMMENT 'tagged node (node_instances->id)',\n" +
                    "  `feature_name` varchar(256) NOT NULL,\n" +
                    "  `feature_value` varchar(256) NOT NULL,\n" +
                    "  KEY `node_id` (`graph_id`,`node_id`),\n" +
                    "  KEY `feature_id` (`feature_name`, `feature_value`)\n" +
                    ") ENGINE=MyISAM DEFAULT CHARSET=latin1 COMMENT='associates tags to nodes';\n" +
                    "\n" +
                    "DROP TABLE IF EXISTS `graph_features`;\n" +
                    "CREATE TABLE `graph_features` (\n" +
                    "  `graph_id` int(11) NOT NULL COMMENT 'tagged graph (graph_instances->id)',\n" +
                    "  `feature_name` varchar(256) NOT NULL,\n" +
                    "  `feature_value` varchar(256) NOT NULL,\n" +
                    "  KEY `graph_id` (`graph_id`),\n" +
                    "  KEY `feature_id` (`feature_name`, `feature_value`)\n" +
                    ") ENGINE=MyISAM DEFAULT CHARSET=latin1 COMMENT='associates tags to nodes';\n" +
                    "\n" +
                    "DROP TABLE IF EXISTS `edge_features`;\n" +
                    "CREATE TABLE `edge_features` (\n" +
                    "  `graph_id` int(11) NOT NULL COMMENT 'tagged graph (graph_instances->id)',\n" +
                    "  `edge_id` int(11) NOT NULL COMMENT 'tagged edge (edge_instances->id)',\n" +
                    "  `feature_name` varchar(256) NOT NULL,\n" +
                    "  `feature_value` varchar(256) NOT NULL,\n" +
                    "  KEY `edge_id` (`graph_id`,`edge_id`),\n" +
                    "  KEY `feature_id` (`feature_name`, `feature_value`)\n" +
                    ") ENGINE=MyISAM DEFAULT CHARSET=latin1 COMMENT='associates tags to nodes';\n";

    private Statement statement;
    private Connection connection;
    private static boolean driverLoaded = false;

    public MysqlDBInterface(String DBHost, String DBName, String DBUsername, String DBPassword, boolean initialize) {
        this(DBHost, DBName, DBUsername, DBPassword);
        if(initialize) {
            logger.info("Initializing Database");
            initializeDatabase();
        }
    }


    public MysqlDBInterface(String DBHost,
                            String DBName,
                            String DBUsername,
                            String DBPassword) {

        if (!driverLoaded) {
            try {
                Class.forName("com.mysql.jdbc.Driver");
                driverLoaded = true;
            } catch (ClassNotFoundException e) {
                e.printStackTrace();
                System.exit(1);
            }
        }

        try {
            this.connection = DriverManager.getConnection("jdbc:mysql://"
                    + DBHost + "/" + DBName, DBUsername, DBPassword);
        } catch (SQLException ex) {
            System.err.println(ex);
        }
        if(connection == null) {
            throw new IllegalStateException("Failed to connect to database");
        }
        try {
            this.statement = connection.createStatement();
        } catch (SQLException ex) {
            Logger.getLogger(MysqlDBInterface.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    @Override
    public boolean isConnected() {
        try {
            return !connection.isClosed();
        } catch(SQLException e) {
            return false;
        }
    }

    @Override
    public void disconnect() {
        try {
            connection.close();
        } catch (SQLException ex) {
            Logger.getLogger(MysqlDBInterface.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    private synchronized List<Map<String, String>> queryDB(String sql, boolean update) {
        List<Map<String, String>> ls = new ArrayList<>();
        try {
            if (update) {
                statement.executeUpdate(sql);
            } else {
                ResultSet rs = statement.executeQuery(sql);
                ResultSetMetaData rsmd = rs.getMetaData();

                while (rs.next()) {
                    Map<String, String> m = new HashMap<>();
                    for (int i = 1; i <= rsmd.getColumnCount(); i++) {
                        m.put(rsmd.getColumnName(i), rs.getString(i));
                    }
                    ls.add(m);
                }
            }
        } catch (SQLException ex) {
            System.err.println(sql);
            Logger.getLogger(MysqlDBInterface.class.getName()).log(Level.SEVERE, null, ex);
        }

        return ls;
    }

    /** executes a UPDATE SQL command on the persistent connection
     *
     * @param sql
     */
    public void updateDB(String sql) {
        queryDB(sql, true);
    }

    /** executes a SELECT SQL statement on the interface's persitent connection
     * and returns a list of the result rows as maps from field name to value.
     * @param sql
     * @return
     */
    public List<Map<String, String>> getQueryResults(String sql) {
        return queryDB(sql, false);
    }

    /** clears any structures in the database and creates the basic tables for SAI
     */
    public void initializeDatabase() {
        for (String statement : INIT_SQL.split(";")) {
            if (!statement.trim().equals("")) {
                updateDB(statement + ";");
            }
        }
    }

    @Override
    public Stream<Integer> getGraphIDStream() {
        Iterator<Integer> i = new Iterator<Integer>() {
            private int id = 0;

            public boolean hasNext() {
                String sql = "SELECT id FROM graph_instances WHERE id > " + id +
                        " ORDER BY id LIMIT 1";
                return getQueryResults(sql).size() > 0;
            }

            public Integer next() {
                String sql = "SELECT id FROM graph_instances WHERE id > " + id +
                        " ORDER BY id LIMIT 1";
                List<Map<String, String>> ls = getQueryResults(sql);
                if (ls.size() > 0) {
                    id = Integer.parseInt(ls.get(0).get("id"));
                    return id;
                }

                throw new IllegalStateException("No elements remaining in iterator");
            }
        };

        return StreamSupport.stream(Spliterators.spliteratorUnknownSize(i, Spliterator.ORDERED), false);
    }

    @Override
    public int addGraph(Graph g) {
        Set<Integer> nodeIds = g.getNodeIDsSet();
        Set<Integer> edgeIds = g.getEdgeIDsSet();
        Set<Feature> features = g.getFeaturesSet();

        updateDB("INSERT INTO graph_instances VALUES (NULL, " +
                nodeIds.size() + ", " + edgeIds.size() + ", " + features.size() + ");");
        int gid = getLastAutoIncrement();

        for (Feature f : features) {
            updateDB("INSERT INTO graph_features VALUES (" +
                    gid + ", '" + f.getName() + "', '" + f.getValue() + "');");
        }
        for (int nid : nodeIds) {

            updateDB("INSERT INTO node_instances VALUES (" +
                    nid + ", " + gid + ", " + g.getNodeFeaturesSet(nid).size() + ");");
            g.getNodeFeatures(nid).forEach(f -> {
                updateDB("INSERT INTO node_features VALUES (" +
                        gid + ", " + nid + ", '" + f.getName() + "', '" + f.getValue() +  "');");
            });
        }
        for (int eid : edgeIds) {
            int source = g.getEdgeSourceNodeID(eid);
            int target = g.getEdgeTargetNodeID(eid);
            updateDB("INSERT INTO edge_instances VALUES (" +
                    eid + ", " + gid + ", " + source + ", " + target + ", " +
                    g.getEdgeFeaturesSet(eid).size() + ");");
            g.getEdgeFeatures(eid).forEach(f -> {
                updateDB("INSERT INTO edge_features VALUES (" +
                        gid + ", " + eid + ", '" + f.getName() + "', '" + f.getValue() + "');");
            });
        }
        return gid;
    }

    @Override
    public void deleteGraph(int graphID) {
        Graph g = retrieveGraph(graphID, ImmutableGraph::new);
        updateDB("DELETE FROM graph_instances WHERE id = " + graphID);
        updateDB("DELETE FROM node_instances WHERE graph_id = " + graphID);
        updateDB("DELETE FROM edge_instances WHERE graph_id = " + graphID);
        updateDB("DELETE FROM graph_features WHERE graph_id = " + graphID);
        updateDB("DELETE FROM graph_indices WHERE graph_id = " + graphID);
        g.getNodeIDs().forEach(nid -> {
            updateDB("DELETE FROM node_features WHERE graph_id = " + graphID +
                    " AND node_id = " + nid);
        });
        g.getEdgeIDs().forEach(eid -> {
            updateDB("DELETE FROM edge_features WHERE graph_id = " + graphID +
                    " AND edge_id = " + eid);
        });
    }

    /** returns the last AUTO_INCREMENT value from the last INSERT SQL statement
     *
     * @return
     */
    public int getLastAutoIncrement() {
        try {
            int newID = 0;
            ResultSet rs = this.statement.getGeneratedKeys();
            if (rs.next()) {
                newID = rs.getInt(1);
            } else {
                throw new RuntimeException("Error creating new graph in database");
            }
            return newID;
        } catch (SQLException ex) {
            Logger.getLogger(MysqlDBInterface.class.getName()).log(Level.SEVERE, null, ex);
        }
        throw new RuntimeException("Error creating new graph in database");
    }

    @Override
    public <G extends Graph> G retrieveGraph(int graphId, GraphFactory<G> f) {
        //insure graph exists in the database
        List<Map<String, String>> rs = getQueryResults("SELECT * FROM graph_instances WHERE id = " + graphId);
        if (rs.size() == 0) {
            throw new IllegalArgumentException("No such graph with id=" + graphId);
        }

        MutableGraph g = new MutableGraph();

        //load graph tags
        rs = getQueryResults("SELECT * FROM graph_features WHERE " +
                "graph_id = "+graphId);
        for(Map<String,String> rm : rs) {
            try {
                g.addFeature(new Feature(rm.get("feature_name"), rm.get("feature_value")));
            } catch(NumberFormatException e) {
                throw new IllegalStateException("corrupt data for graph #"+graphId + " data: " + rm);
            }
        }


        //load nodes
        rs = getQueryResults("SELECT * FROM node_instances WHERE " +
                "graph_id = "+graphId);

        for(Map<String,String> rm : rs) {
            int nid = Integer.parseInt(rm.get("id"));
            List<Map<String,String>> tagrs =
                    getQueryResults("SELECT * FROM node_features "+
                            "WHERE node_id = "+
                            nid + " AND " +
                            "graph_id = " + graphId);

            g.addNode(nid);

            for(Map<String,String> tagrm : tagrs)
                g.addNodeFeature(nid, new Feature(tagrm.get("feature_name"), tagrm.get("feature_value")));
        }

        //load edges
        rs = getQueryResults("SELECT * FROM edge_instances WHERE " +
                "graph_id = "+graphId);
        for(Map<String,String> rm : rs) {
            int eid = Integer.parseInt(rm.get("id"));
            List<Map<String,String>> tagrs =
                    getQueryResults("SELECT * FROM edge_features "+
                            "WHERE edge_id = "+eid + " AND " +
                            "graph_id = " + graphId);

            g.addEdge(eid,
                    Integer.parseInt(rm.get("from_node_id")),
                    Integer.parseInt(rm.get("to_node_id")));

            for(Map<String,String> tagrm : tagrs) {
                g.addEdgeFeature(eid, new Feature(tagrm.get("feature_name"), tagrm.get("feature_value")));
            }
        }

        return f.copy(g);
    }

    /** returns the number of graph structures in the database */
    public int getDatabaseSize() {
        return Integer.parseInt(
                getQueryResults("SELECT COUNT(*) FROM graph_instances").get(0).get("COUNT(*)"));
    }


    /** returns the number of nodes in the database */
    public double getNodesInDatabase() {
        return Integer.parseInt(
                getQueryResults("SELECT COUNT(*) FROM graph_instances gi, node_instances ni WHERE "+
                        "gi.id = ni.graph_id").get(0).get("COUNT(*)"));
    }

    /** returns the number of edges in the database */
    public double getEdgesInDatabase() {
        return Integer.parseInt(
                getQueryResults("SELECT COUNT(*) FROM graph_instances gi, edge_instances ni WHERE "+
                        "gi.id = ni.graph_id").get(0).get("COUNT(*)"));
    }

    @Override
    public Stream<Integer> retrieveGraphsWithFeatureName(String name) {
        Set<Integer> s = new HashSet<Integer>();
        for (Map<String, String> m :
                getQueryResults("SELECT gi.id FROM graph_instances gi, graph_features fi WHERE " +
                        " fi.graph_id = gi.id AND fi.feature_name = " + name)) {
            s.add(Integer.parseInt(m.get("id")));
        }
        for (Map<String, String> m :
                getQueryResults("SELECT gi.id FROM graph_instances gi, edge_features fi WHERE " +
                        " fi.graph_id = gi.id AND fi.feature_name = " + name)) {
            s.add(Integer.parseInt(m.get("id")));
        }
        for (Map<String, String> m :
                getQueryResults("SELECT gi.id FROM graph_instances gi, node_features fi WHERE " +
                        " fi.graph_id = gi.id AND fi.feature_name = " + name)) {
            s.add(Integer.parseInt(m.get("id")));
        }

        return s.stream();
    }

    @Override
    public Stream<Integer> retrieveGraphsWithFeature(Feature f) {
        Set<Integer> s = new HashSet<Integer>();
        for (Map<String, String> m :
                getQueryResults("SELECT gi.id FROM graph_instances gi, graph_features fi WHERE " +
                        " fi.graph_id = gi.id AND fi.feature_name = " + f.getName() +
                        " AND fi.feature_value = " + f.getValue())) {
            s.add(Integer.parseInt(m.get("id")));
        }
        for (Map<String, String> m :
                getQueryResults("SELECT gi.id FROM graph_instances gi, edge_features fi WHERE " +
                        " fi.graph_id = gi.id AND fi.feature_name = " + f.getName() +
                        " AND fi.feature_value = " + f.getValue())) {
            s.add(Integer.parseInt(m.get("id")));
        }
        for (Map<String, String> m :
                getQueryResults("SELECT gi.id FROM graph_instances gi, node_features fi WHERE " +
                        " fi.graph_id = gi.id AND fi.feature_name = " + f.getName() +
                        " AND fi.feature_value = " + f.getValue())) {
            s.add(Integer.parseInt(m.get("id")));
        }

        return s.stream();
    }
}
