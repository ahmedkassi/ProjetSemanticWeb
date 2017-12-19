import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import com.google.transit.realtime.GtfsRealtime;
import com.google.transit.realtime.GtfsRealtime.FeedEntity;
import com.google.transit.realtime.GtfsRealtime.FeedMessage;
import org.apache.jena.rdf.model.*;
import org.apache.jena.vocabulary.LocationMappingVocab;

public class GtfsRealtimeExample {

    private static boolean _dynamicRefreshInterval = true;
    private static long _mostRecentRefresh = -1;
    private static int _refreshInterval = 20;
    private static Map<String, String> _vehicleIdsByEntityIds = new HashMap<String, String>();

    private static Map<String, Vehicle> _vehiclesById = new ConcurrentHashMap<String, Vehicle>();

    private static List<VehicleListener> _listeners = new CopyOnWriteArrayList<VehicleListener>();
    private static ScheduledExecutorService _executor;
    private static final RefreshTask _refreshTask = new RefreshTask();
    private static class RefreshTask implements Runnable {


        public void run() {
            try {
                refresh();
            } catch (Exception ex) {

            }
        }
    }
    private static String getVehicleId(GtfsRealtime.VehiclePosition vehicle) {
        if (!vehicle.hasVehicle()) {
            return null;
        }
        GtfsRealtime.VehicleDescriptor desc = vehicle.getVehicle();

            return desc.getLabel();


    }

    private static boolean processDataset(FeedMessage feed) {

        List<Vehicle> vehicles = new ArrayList<Vehicle>();
        boolean update = false;

        for (FeedEntity entity : feed.getEntityList()) {
            if (entity.hasIsDeleted() && entity.getIsDeleted()) {
                String vehicleId = _vehicleIdsByEntityIds.get(entity.getId());
                if (vehicleId == null) {

                    continue;
                }
                _vehiclesById.remove(vehicleId);
                continue;
            }
            if (!entity.hasVehicle()) {
                continue;
            }
            GtfsRealtime.VehiclePosition vehicle = entity.getVehicle();
            String vehicleId = getVehicleId(vehicle);
            if (vehicleId == null) {
                continue;
            }
            _vehicleIdsByEntityIds.put(entity.getId(), vehicleId);
            if (!vehicle.hasPosition()) {
                continue;
            }
            GtfsRealtime.Position position = vehicle.getPosition();
            Vehicle v = new Vehicle();
            v.setId(vehicleId);
            v.setLat(position.getLatitude());
            v.setLon(position.getLongitude());
            v.setLastUpdate(System.currentTimeMillis());

            Vehicle existing = _vehiclesById.get(vehicleId);
            if (existing == null || existing.getLat() != v.getLat()
                    || existing.getLon() != v.getLon()) {
                _vehiclesById.put(vehicleId, v);
                System.out.println(v);
                update = true;
            } else {
                v.setLastUpdate(existing.getLastUpdate());
            }

            vehicles.add(v);
        }

        if (update) {

        }

        for (VehicleListener listener : _listeners) {
            listener.handleVehicles(vehicles);
        }

        return update;
    }

    private static void updateRefreshInterval() {
        long t = System.currentTimeMillis();
        if (_mostRecentRefresh != -1) {
            int refreshInterval = (int) ((t - _mostRecentRefresh) / (2 * 1000));
            _refreshInterval = Math.max(10, refreshInterval);
        }
        _mostRecentRefresh = t;
    }
    public static void refresh() throws IOException {


        URL url = new URL(" http://gtfs.ovapi.nl/nl/vehiclePositions.pb");
        FeedMessage feed = FeedMessage.parseFrom(url.openStream());
        boolean hadUpdate = processDataset(feed);
        if (hadUpdate) {
            if (_dynamicRefreshInterval) {
                updateRefreshInterval();
            }
            _executor.schedule(_refreshTask, _refreshInterval, TimeUnit.SECONDS);
        }
    }





    public static void main(String[] args) throws Exception {
    	//jena
        Model model = ModelFactory.createDefaultModel();
        Map<String, String> vehicleIdsByEntityIds = new HashMap<String, String>();
        Map<String, Vehicle> vehiclesById = new ConcurrentHashMap<String, Vehicle>();
        URL url = new URL("http://developer.mbta.com/lib/GTRTFS/Alerts/VehiclePositions.pb");
       Property geo = ResourceFactory.createProperty("http://www.w3.org/2003/01/geo/wgs84_pos#") ;
        Property lat = ResourceFactory.createProperty("http://www.w3.org/2003/01/geo/wgs84_pos#lat") ;
        Property lon = ResourceFactory.createProperty("http://www.w3.org/2003/01/geo/wgs84_pos#long") ;

        while(true){
        FeedMessage feed = FeedMessage.parseFrom(url.openStream());
        for (FeedEntity entity : feed.getEntityList()) {
            if (entity.hasVehicle()) {
                GtfsRealtime.VehiclePosition vehicle = entity.getVehicle();
                String vehicleId = getVehicleId(vehicle);
                vehicleIdsByEntityIds.put(entity.getId(), vehicleId);
                GtfsRealtime.Position position = vehicle.getPosition();
                Vehicle v = new Vehicle();
                v.setId(vehicleId);
                v.setLat(position.getLatitude());
                v.setLon(position.getLongitude());
                v.setLastUpdate(System.currentTimeMillis());
               Vehicle existing = vehiclesById.get(vehicleId);
               String   vehicleURI ="http://somewhere/vehicle"+vehicleId;
               if (existing == null ) {
               
                    
                  
                   
                    Resource vehiclerdf = model.createResource(vehicleURI)
                            .addProperty(lat,Double.toString(v.getLat()))
                            .addProperty(lon, Double.toString( v.getLon()));

                    vehiclesById.put(vehicleId, v);
                    System.out.println(v.getId());
                    System.out.println(v.getLat());
                    System.out.println(v.getLon());
                    System.out.println(vehiclerdf.toString());
                    System.out.println("-------------------------------------------------------------");
               }
               Resource r=model.getResource(vehicleURI);
               if(r.getProperty(lat).getDouble() != v.getLat()){
                   r.removeAll(lon);
                   r.addProperty(lon, Double.toString(v.getLon()));
                   }
                   if(r.getProperty(lon).getDouble() != v.getLon()){
                   	r.removeAll(lat);
                   	r.addProperty(lat, Double.toString(v.getLat()));
                   }// list the statements in the Model
                 //   StmtIterator iter = model.listStatements();

            // print out the predicate, subject and object of each statement
                 /*   while (iter.hasNext()) {
                    	     Statement stmt      = iter.nextStatement();  // get next statement
                        Resource  subject   = stmt.getSubject();     // get the subject
                        Property  predicate = stmt.getPredicate();   // get the predicate
                        RDFNode   object    = stmt.getObject();      // get the object

                        System.out.print(subject.toString());
                        System.out.print(" " + predicate.toString() + " ");
                        if (object instanceof Resource) {
                            System.out.print(object.toString());
                        } else {
                            // object is a literal
                            System.out.print(" \"" + object.toString() + "\"");
                        }

                        System.out.println(" .");
                    }*/
                model.write(System.out);
                
                
            }
        }
            Thread t = new Thread() {
                public void run() {
                    System.out.println("waiting 30 sec..............");
                }
            };
            t.start();
            Thread.sleep(30000);

       }




    }

}

