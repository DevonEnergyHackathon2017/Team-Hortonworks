package com.hortonworks;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

import org.geojson.Feature;
import org.geojson.FeatureCollection;
import org.geojson.Point;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Root resource (exposed at "myresource" path)
 */
@Path("myresource")
public class MyResource {

	/**
	 * Method handling HTTP GET requests. The returned object will be sent
	 * to the client as "text/plain" media type.
	 *
	 * @return String that will be returned as a text/plain response.
	 */
	@GET
	@Produces(MediaType.TEXT_PLAIN)
	public String getIt() {
		return "Got it!";
	}

	@GET
	@Path("hello")
	@Produces(MediaType.TEXT_PLAIN)
	public String helloWorld() {
		return "Hello, world!";
	}

	@GET
	@Path("geojson")
	@Produces(MediaType.APPLICATION_JSON)
	public String foo() throws JsonProcessingException, SQLException {

		FeatureCollection fc = new FeatureCollection();

		try {
			String jdbcurl = "jdbc:phoenix:thin:url=http://hdp01.woolford.io:8765;serialization=PROTOBUF";
			Connection con = DriverManager.getConnection(jdbcurl);


			//query for the last 60 seconds of records
			String query = "select * from movement where ts > " + (System.currentTimeMillis()-60000) + " order by id, ts";
			PreparedStatement stmt = con.prepareStatement(query);
			ResultSet rset = stmt.executeQuery();
			String id = "";
			while (rset.next()) {
				//id, ts, lat, lon, siteid
				if(!id.equals(rset.getString(1))) {
					id = rset.getString(1);
					Point p = new Point(rset.getDouble(4), rset.getDouble(3), 0d);
					Feature f = new Feature();
					f.setGeometry(p);
					f.setProperty("id", id);
					f.setProperty("siteId", rset.getInt(5));
					f.setProperty("ts", new Timestamp(rset.getLong(2)));
					fc.add(f);
				}

			}
		}catch(SQLException e) {
			e.printStackTrace();
		}

		return new ObjectMapper().writeValueAsString(fc);

	}
}
