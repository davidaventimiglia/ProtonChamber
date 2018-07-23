package org.protonchamber;

import java.io.*;
import java.sql.*;
import java.util.*;
import javax.servlet.*;
import org.apache.olingo.commons.api.data.*;
import org.apache.olingo.commons.api.edm.*;
import org.apache.olingo.commons.api.edm.constants.*;
import org.apache.olingo.commons.api.format.*;
import org.apache.olingo.commons.api.http.*;
import org.apache.olingo.server.api.*;
import org.apache.olingo.server.api.deserializer.*;
import org.apache.olingo.server.api.processor.*;
import org.apache.olingo.server.api.serializer.*;
import org.apache.olingo.server.api.uri.*;

public class ProtonEntityProcessor implements EntityProcessor {
    OData odata;
    ServiceMetadata serviceMetaData;
    Connection conn;
    GenericServlet servlet;

    public ProtonEntityProcessor (Connection conn, GenericServlet servlet) {
	this.conn = conn;
	this.servlet = servlet;}

    @Override
    public void init (OData odata, ServiceMetadata serviceMetaData) {
	this.odata = odata;
	this.serviceMetaData = serviceMetaData;}

    static class AutoCloseableWrapper<T> implements AutoCloseable {
	T wrapped;
	public AutoCloseableWrapper (T wrapped) {
	    this.wrapped = wrapped;}
	@Override
	public void close () {}
	public T getWrapped () {return wrapped;}}

    @Override
    public void createEntity (ODataRequest request, ODataResponse response, UriInfo uriInfo, ContentType requestFormat, ContentType responseFormat) throws ODataApplicationException, DeserializerException, SerializerException {
	List<UriResource> parts = uriInfo.getUriResourceParts();
	UriResourceEntitySet uriResourceEntitySet = (UriResourceEntitySet)parts.get(0);
	EdmEntitySet edmEntitySet = uriResourceEntitySet.getEntitySet();
	EdmEntityType edmEntityType = edmEntitySet.getEntityType();
	InputStream requestInputStream = request.getBody();
	ODataDeserializer deserializer = odata.createDeserializer(requestFormat);
	DeserializerResult result = deserializer.entity(requestInputStream, edmEntityType);
	Entity requestEntity = result.getEntity();
	List<String> names = new ArrayList<>();
	List<String> values = new ArrayList<>();
	for (Property p : requestEntity.getProperties()) {names.add(p.getName()); values.add(""+p.getValue());}
	String insert = String.format("insert into %s (%s) values (%s)", edmEntitySet.getName(), String.join(",", names), String.join(",", values));
	try (Statement s = conn.createStatement();
	     AutoCloseableWrapper<Integer> rowCount = new AutoCloseableWrapper<>(s.executeUpdate(insert, names.toArray(new String[0])));
	     ResultSet r = s.getGeneratedKeys()) {
	    ResultSetMetaData m = r.getMetaData();
	    Set<String> columnNames = new HashSet<>();
	    for (int i=1; i<=m.getColumnCount(); i++) columnNames.add(m.getColumnName(i));
	    EntityCollection c = new EntityCollection();
	    List<Entity> entityList = c.getEntities();
	    List<String> propertyNames = edmEntitySet.getEntityType().getPropertyNames();
	    while (r.next()) {
		Entity e = requestEntity;
		for (String p : propertyNames)
		    if (columnNames.contains(p))
			e.addProperty(new Property(null, p, ValueType.PRIMITIVE,
						   edmEntitySet
						   .getEntityType()
						   .getProperty(p)
						   .getType()
						   .getKind()==EdmTypeKind.PRIMITIVE ?
						   ((EdmPrimitiveType)
						    (edmEntitySet
						     .getEntityType()
						     .getProperty(p)
						     .getType()))
						   .getDefaultType()
						   .isAssignableFrom(Integer.class) ?
						   r.getInt(p) :
						   r.getString(p) :
						   r.getString(p)));
		entityList.add(e);
		ContextURL contextUrl = ContextURL.with().entitySet(edmEntitySet).build();
		EntitySerializerOptions options = EntitySerializerOptions.with().contextURL(contextUrl).build();
		ODataSerializer serializer = odata.createSerializer(responseFormat);
		SerializerResult serializedResponse = serializer.entity(serviceMetaData, edmEntityType, e, options);
		response.setContent(serializedResponse.getContent());
		response.setStatusCode(HttpStatusCode.CREATED.getStatusCode());
		response.setHeader(HttpHeader.CONTENT_TYPE, responseFormat.toContentTypeString());
		break;}}
	catch (Exception e) {
	    servlet.log(e.getMessage(), e);
	    throw new ODataApplicationException(String.format("message: %s, query: %s", e.getMessage(), insert), 500, Locale.US);}}

    @Override
    public void deleteEntity (ODataRequest request, ODataResponse response, UriInfo uriInfo) {
	throw new UnsupportedOperationException();}

    @Override
    public void updateEntity(ODataRequest request, ODataResponse response, UriInfo uriInfo, ContentType requestFormat, ContentType responseFormat) {
	throw new UnsupportedOperationException();}

    @Override
    public void readEntity (ODataRequest request,
			    ODataResponse response,
			    UriInfo uriInfo,
			    ContentType responseFormat)
	throws ODataApplicationException,
	       ODataLibraryException {
	List<UriResource> parts = uriInfo.getUriResourceParts();
	UriResourceEntitySet uriResourceEntitySet = (UriResourceEntitySet)parts.get(0);
	EdmEntitySet edmEntitySet = uriResourceEntitySet.getEntitySet();
	List<UriParameter> keyPredicates = uriResourceEntitySet.getKeyPredicates();
	List<String> sqlPredicates = new ArrayList<>();
	for (UriParameter p : keyPredicates) sqlPredicates.add(String.format("%s=%s", p.getName(), p.getText()));
	String query = String.format("select * from %s where true and %s", edmEntitySet.getName(), String.join("and", sqlPredicates));
	try (Statement s = conn.createStatement();
	     ResultSet r = s.executeQuery(query)) {
	    EntityCollection c = new EntityCollection();
	    List<Entity> entityList = c.getEntities();
	    List<String> propertyNames = edmEntitySet.getEntityType().getPropertyNames();
	    while (r.next()) {
		Entity e = new Entity();
		for (String p : propertyNames) e.addProperty(new Property(null, p, ValueType.PRIMITIVE,
									  edmEntitySet
									  .getEntityType()
									  .getProperty(p)
									  .getType()
									  .getKind()==EdmTypeKind.PRIMITIVE ?
									  ((EdmPrimitiveType)
									   (edmEntitySet
									    .getEntityType()
									    .getProperty(p)
									    .getType()))
									  .getDefaultType()
									  .isAssignableFrom(Integer.class) ?
									  r.getInt(p) :
									  r.getString(p) :
									  r.getString(p)));
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
	    throw new ODataApplicationException(String.format("message: %s, query: %s", e.getMessage(), query), 500, Locale.US);}}}
