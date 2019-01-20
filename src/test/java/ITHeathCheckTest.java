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
    	try (Connection c = DriverManager.getConnection("jdbc:postgresql://localhost:5432/atomic", "atomic", "atomic");
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
	try (Connection c = DriverManager.getConnection("jdbc:postgresql://localhost:5432/atomic", "atomic", "atomic");
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
    	assertEquals(HttpURLConnection.HTTP_OK, response.statusCode());}

    @Test
    public void metadataDocumentCorrect () throws URISyntaxException, IOException, InterruptedException {
        // try {this.transformer = TransformerFactory.newInstance().newTransformer(xslt);}
        // catch (Throwable t) {throw new ServletException(t);}}
        // try {transformer.transform(new StreamSource(getInputStream()), new StreamResult(target));}
        // catch (Throwable t) {throw new IOException(t);}
    	HttpRequest request = HttpRequest.newBuilder()
    	    .uri(new URI("http://localhost:9080/ProtonChamber/ProtonService.svc/$metadata"))
    	    .GET()
    	    .build();
    	HttpResponse<String> response = HttpClient.newBuilder()
    	    .proxy(ProxySelector.getDefault())
    	    .build()
    	    .send(request, HttpResponse.BodyHandlers.ofString());
	String targetXml = response.body();
	String controlXml = "<?xml version='1.0' encoding='UTF-8'?><edmx:Edmx Version=\"4.0\" xmlns:edmx=\"http://docs.oasis-open.org/odata/ns/edmx\"><edmx:DataServices><Schema xmlns=\"http://docs.oasis-open.org/odata/ns/edm\" Namespace=\"public\"><EntityType Name=\"person\"><Key><PropertyRef Name=\"id\" Alias=\"id\"/></Key><Property Name=\"name\" Type=\"Edm.String\" Precision=\"2147483647\" Scale=\"0\"/><Property Name=\"id\" Type=\"Edm.Int32\" DefaultValue=\"nextval(&apos;person_id_seq&apos;::regclass)\" Precision=\"10\" Scale=\"0\"/></EntityType><EntityContainer Name=\"EntityContainer\"><EntitySet Name=\"person\" EntityType=\"public.person\"/></EntityContainer></Schema></edmx:DataServices></edmx:Edmx>";
	assertEquals(controlXml, targetXml);}}
    
