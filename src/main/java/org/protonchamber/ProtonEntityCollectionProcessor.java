package org.protonchamber;

import java.io.*;
import java.sql.*;
import java.util.*;
import javax.servlet.*;
import org.apache.olingo.commons.api.data.*;
import org.apache.olingo.commons.api.edm.*;
import org.apache.olingo.commons.api.format.*;
import org.apache.olingo.commons.api.http.*;
import org.apache.olingo.server.api.*;
import org.apache.olingo.server.api.processor.*;
import org.apache.olingo.server.api.serializer.*;
import org.apache.olingo.server.api.uri.*;

public class ProtonEntityCollectionProcessor implements EntityCollectionProcessor {
    OData odata;
    ServiceMetadata serviceMetaData;
    Connection conn;
    GenericServlet servlet;

    public ProtonEntityCollectionProcessor (Connection conn, GenericServlet servlet) {
	this.conn = conn;
	this.servlet = servlet;}

    @Override
    public void init (OData odata, ServiceMetadata serviceMetaData) {
	this.odata = odata;
	this.serviceMetaData = serviceMetaData;}

    @Override
    public void readEntityCollection (ODataRequest request,
				      ODataResponse response,
				      UriInfo uriInfo,
				      ContentType responseFormat)
	throws ODataApplicationException,
	       ODataLibraryException {
	List<UriResource> parts = uriInfo.getUriResourceParts();
	UriResourceEntitySet uriResourceEntitySet = (UriResourceEntitySet)parts.get(0);
	EdmEntitySet edmEntitySet = uriResourceEntitySet.getEntitySet();
	servlet.log("What the fuck is going on????");
	try (Statement s = conn.createStatement();
	     ResultSet r = s.executeQuery(String.format("select * from %s", edmEntitySet.getName()))) {
	    EntityCollection c = new EntityCollection();
	    List<Entity> entityList = c.getEntities();
	    List<String> propertyNames = edmEntitySet.getEntityType().getPropertyNames();
	    while (r.next()) {
		Entity e = new Entity();
		for (String p : propertyNames) {
		    e.addProperty(new Property(null, p, ValueType.PRIMITIVE, r.getString(p)));}
		entityList.add(e);}
	    ODataSerializer serializer = odata.createSerializer(responseFormat);
	    EdmEntityType edmEntityType = edmEntitySet.getEntityType();
	    ContextURL contextUrl = ContextURL.with().entitySet(edmEntitySet).build();
	    final String id = request.getRawBaseUri() + "/" + edmEntitySet.getName();
	    EntityCollectionSerializerOptions opts = EntityCollectionSerializerOptions.with().id(id).contextURL(contextUrl).build();
	    SerializerResult serializerResult = serializer.entityCollection(serviceMetaData, edmEntityType, c, opts);
	    InputStream serializedContent = serializerResult.getContent();
	    response.setContent(serializedContent);
	    response.setStatusCode(HttpStatusCode.OK.getStatusCode());
	    response.setHeader(HttpHeader.CONTENT_TYPE, responseFormat.toContentTypeString());}
	catch (Exception e) {
	    servlet.log(e.getMessage(), e);
	    throw new ODataApplicationException(e.getMessage(), 500, Locale.US);}}}

