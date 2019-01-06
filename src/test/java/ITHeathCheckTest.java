package org.protonchamber;

import java.io.*;
import java.net.*;
import java.net.http.*;
import java.sql.*;
import org.junit.jupiter.api.*;
import org.stringtemplate.v4.*;
import static org.junit.jupiter.api.Assertions.*;

class ITHealthCheckTest {
    @BeforeAll
    public static void setUp () {
    	ST st;
    	STGroup g = new STGroupFile("default.stg");
    	st = g.getInstanceOf("setUp");
    	try (Connection c = DriverManager.getConnection("jdbc:postgresql://localhost:7432/atomic", "atomic", "atomic");
    	     Statement s = c.createStatement()) {
    	    s.execute(st.render());}
    	catch (Exception e) {
    	    throw new RuntimeException(e);}}
    
    @Test
    public void myFirstTest () {
        assertEquals(2, 1 + 1);}

    @Test
    public void dbSantityCheck () {
	ST st;
	STGroup g = new STGroupFile("default.stg");
	st = g.getInstanceOf("test");
	try (Connection c = DriverManager.getConnection("jdbc:postgresql://localhost:7432/atomic", "atomic", "atomic");
	     Statement s = c.createStatement();
	     ResultSet r = s.executeQuery(st.render())) {
	    DatabaseMetaData m = c.getMetaData();}
	catch (Exception e) {
	    throw new RuntimeException(e);}}

    @Test
    public void metadataDocumentExists () throws URISyntaxException, IOException, InterruptedException {
    	HttpRequest request = HttpRequest.newBuilder()
    	    .uri(new URI("http://localhost:9080/ProtonChamber/ProtonService.svc/$metadata"))
    	    .GET()
    	    .build();
    	HttpResponse<String> response = HttpClient.newBuilder()
    	    .proxy(ProxySelector.getDefault())
    	    .build()
    	    .send(request, HttpResponse.BodyHandlers.ofString());
    	assertEquals(HttpURLConnection.HTTP_OK, response.statusCode());}}
