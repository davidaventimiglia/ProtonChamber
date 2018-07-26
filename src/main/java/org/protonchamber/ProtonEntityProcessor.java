package org.protonchamber;

import java.io.*;
import java.sql.*;
import java.util.*;
import javax.servlet.*;
import javax.sql.*;
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
    DataSource ds;
    GenericServlet servlet;

    public ProtonEntityProcessor (DataSource ds, GenericServlet servlet) {
	this.ds = ds;
	this.servlet = servlet;}

    @Override
    public void init (OData odata, ServiceMetadata serviceMetaData) {
	this.odata = odata;
	this.serviceMetaData = serviceMetaData;}

    static class AutoCloseableWrapper<T> implements AutoCloseable {
	T wrapped;
	public AutoCloseableWrapper (T wrapped) {this.wrapped = wrapped;}
	@Override
	public void close () {}
	public T getWrapped () {return wrapped;}}

    @Override
    public void createEntity (ODataRequest request, ODataResponse response, UriInfo uriInfo, ContentType requestFormat, ContentType responseFormat) throws ODataApplicationException, DeserializerException, SerializerException {
	for (UriResource ur : uriInfo.getUriResourceParts()) {
	    if (ur instanceof UriResourceEntitySet) {
		EdmEntitySet edmEntitySet = ((UriResourceEntitySet)ur).getEntitySet();
		EdmEntityType edmEntityType = edmEntitySet.getEntityType();
		Entity requestEntity = odata.createDeserializer(requestFormat).entity(request.getBody(), edmEntityType).getEntity();
		List<String> names = new ArrayList<>();
		List<String> values = new ArrayList<>();
		for (Property p : requestEntity.getProperties()) {names.add(p.getName()); values.add(""+p.getValue());}
		String insert = String.format("insert into %s (%s) values (%s)", edmEntitySet.getName(), String.join(",", names), String.join(",", values));
		try (Connection c = ds.getConnection();
		     Statement s = c.createStatement();
		     AutoCloseableWrapper<Integer> rowCount = new AutoCloseableWrapper<>(s.executeUpdate(insert, edmEntityType.getPropertyNames().toArray(new String[0])));
		     ResultSet r = s.getGeneratedKeys()) {
		    ResultSetMetaData m = r.getMetaData();
		    Set<String> columnNames = new HashSet<>();
		    for (int i=1; i<=m.getColumnCount(); i++) columnNames.add(m.getColumnName(i));
		    EntityCollection ec = new EntityCollection();
		    List<Entity> entityList = ec.getEntities();
		    while (r.next()) {
			Entity e = requestEntity;
			for (int i=1; i<=m.getColumnCount(); i++)
			    if (e.getProperty(m.getColumnName(i))==null)
				e.addProperty(new Property(null, m.getColumnName(i), ValueType.PRIMITIVE,
							   edmEntitySet
							   .getEntityType()
							   .getProperty(m.getColumnName(i))
							   .getType()
							   .getKind()==EdmTypeKind.PRIMITIVE ?
							   ((EdmPrimitiveType)
							    (edmEntitySet
							     .getEntityType()
							     .getProperty(m.getColumnName(i))
							     .getType()))
							   .getDefaultType()
							   .isAssignableFrom(Integer.class) ?
							   r.getInt(m.getColumnName(i)) :
							   r.getString(m.getColumnName(i)) :
							   r.getString(m.getColumnName(i)))); else
				e.getProperty(m.getColumnName(i)).setValue(ValueType.PRIMITIVE,
									   edmEntitySet
									   .getEntityType()
									   .getProperty(m.getColumnName(i))
									   .getType()
									   .getKind()==EdmTypeKind.PRIMITIVE ?
									   ((EdmPrimitiveType)
									    (edmEntitySet
									     .getEntityType()
									     .getProperty(m.getColumnName(i))
									     .getType()))
									   .getDefaultType()
									   .isAssignableFrom(Integer.class) ?
									   r.getInt(m.getColumnName(i)) :
									   r.getString(m.getColumnName(i)) :
									   r.getString(m.getColumnName(i)));
			entityList.add(e);
			response.setContent(odata.createSerializer(responseFormat).entity(serviceMetaData, edmEntityType, e, EntitySerializerOptions.with().contextURL(ContextURL.with().entitySet(edmEntitySet).build()).build()).getContent());
			response.setStatusCode(HttpStatusCode.CREATED.getStatusCode());
			response.setHeader(HttpHeader.CONTENT_TYPE, responseFormat.toContentTypeString());
			return;}}
		catch (Exception e) {
		    servlet.log(e.getMessage(), e);
		    throw new ODataApplicationException(String.format("message: %s, query: %s", e.getMessage(), insert), 500, Locale.US);}}
	    throw new IllegalStateException("Should never get here");}}

    @Override
    public void deleteEntity (ODataRequest request, ODataResponse response, UriInfo uriInfo)
	throws ODataApplicationException {
	List<UriResource> parts = uriInfo.getUriResourceParts();
	UriResourceEntitySet uriResourceEntitySet = (UriResourceEntitySet)parts.get(0);
	HttpMethod httpMethod = request.getMethod();
	EdmEntitySet edmEntitySet = uriResourceEntitySet.getEntitySet();
	EdmEntityType edmEntityType = edmEntitySet.getEntityType();
	InputStream requestInputStream = request.getBody();
	List<UriParameter> keyPredicates = uriResourceEntitySet.getKeyPredicates();
	List<String> sqlPredicates = new ArrayList<>();
	for (UriParameter p : keyPredicates) sqlPredicates.add(String.format("%s=%s", p.getName(), String.format("'%s'", p.getText())));
	String delete = String.format("delete from %s where true and %s", edmEntitySet.getName(), String.join("and", sqlPredicates));
	try (Connection c = ds.getConnection();
	     Statement s = c.createStatement();
	     AutoCloseableWrapper<Integer> rowCount = new AutoCloseableWrapper<>(s.executeUpdate(delete))) {
	    response.setStatusCode(HttpStatusCode.NO_CONTENT.getStatusCode());}
	catch (Exception e) {
	    servlet.log(e.getMessage(), e);
	    throw new ODataApplicationException(String.format("message: %s, query: %s", e.getMessage(), delete), 500, Locale.US);}}

    @Override
    public void updateEntity(ODataRequest request, ODataResponse response, UriInfo uriInfo, ContentType requestFormat, ContentType responseFormat) throws ODataApplicationException, DeserializerException {
	List<UriResource> parts = uriInfo.getUriResourceParts();
	UriResourceEntitySet uriResourceEntitySet = (UriResourceEntitySet)parts.get(0);
	HttpMethod httpMethod = request.getMethod();
	EdmEntitySet edmEntitySet = uriResourceEntitySet.getEntitySet();
	EdmEntityType edmEntityType = edmEntitySet.getEntityType();
	InputStream requestInputStream = request.getBody();
	ODataDeserializer deserializer = odata.createDeserializer(requestFormat);
	DeserializerResult result = deserializer.entity(requestInputStream, edmEntityType);
	Entity requestEntity = result.getEntity();
	Map<String, String> pairs = new HashMap<>();
	List<UriParameter> keyPredicates = uriResourceEntitySet.getKeyPredicates();
	List<String> sqlPredicates = new ArrayList<>();
	for (UriParameter p : keyPredicates) sqlPredicates.add(String.format("%s=%s", p.getName(), String.format("'%s'", p.getText())));
	for (Property p : requestEntity.getProperties()) pairs.put(p.getName(), ""+p.getValue());
	String update = String.format("update %s set %s where true and %s", edmEntitySet.getName(), pairs.toString().replace("}","").replace("{",""), String.join("and", sqlPredicates));
	try (Connection c = ds.getConnection();
	     Statement s = c.createStatement();
	     AutoCloseableWrapper<Integer> rowCount = new AutoCloseableWrapper<>(s.executeUpdate(update))) {
	    response.setStatusCode(HttpStatusCode.NO_CONTENT.getStatusCode());}
	catch (Exception e) {
	    servlet.log(e.getMessage(), e);
	    throw new ODataApplicationException(String.format("message: %s, query: %s", e.getMessage(), update), 500, Locale.US);}}

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
	try (Connection c = ds.getConnection();
	     Statement s = c.createStatement();
	     ResultSet r = s.executeQuery(query)) {
	    EntityCollection ec = new EntityCollection();
	    List<Entity> entityList = ec.getEntities();
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
	    SerializerResult serializerResult = serializer.entityCollection(serviceMetaData, edmEntityType, ec, opts);
	    InputStream serializedContent = serializerResult.getContent();
	    response.setContent(serializedContent);
	    response.setStatusCode(HttpStatusCode.OK.getStatusCode());
	    response.setHeader(HttpHeader.CONTENT_TYPE, responseFormat.toContentTypeString());}
	catch (Exception e) {
	    servlet.log(e.getMessage(), e);
	    throw new ODataApplicationException(String.format("message: %s, query: %s", e.getMessage(), query), 500, Locale.US);}}}
