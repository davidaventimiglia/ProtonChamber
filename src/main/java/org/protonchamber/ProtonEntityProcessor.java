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
	for (UriResource part : uriInfo.getUriResourceParts())
	    if (part.getKind()==UriResourceKind.entitySet) {
		UriResourceEntitySet es = (UriResourceEntitySet)part;
		Entity e = odata.createDeserializer(requestFormat).entity(request.getBody(), es.getEntityType()).getEntity();
		List<String> names = new ArrayList<>(); List<String> values = new ArrayList<>();
		for (Property p : e.getProperties()) {names.add(p.getName()); values.add(""+p.getValue());}
		String insert = String.format("insert into %s (%s) values (%s)", es.getEntitySet().getName(), String.join(",", names), String.join(",", values));
		try (Connection c = ds.getConnection();
		     Statement s = c.createStatement();
		     AutoCloseableWrapper<Integer> rowCount = new AutoCloseableWrapper<>(s.executeUpdate(insert, es.getEntityType().getPropertyNames().toArray(new String[0])));
		     ResultSet r = s.getGeneratedKeys()) {
		    while (r.next())
			for (int i=1; i<=r.getMetaData().getColumnCount(); i++)
			    if (es.getEntityType().getProperty(r.getMetaData().getColumnName(i)).getType().getKind()==EdmTypeKind.PRIMITIVE)
				if (e.getProperty(r.getMetaData().getColumnName(i))==null)
				    e.addProperty(new Property(es.getEntityType().getProperty(r.getMetaData().getColumnName(i)).getType().getName(), r.getMetaData().getColumnName(i), ValueType.PRIMITIVE, r.getObject(i)));
				else
				    e.getProperty(r.getMetaData().getColumnName(i)).setValue(ValueType.PRIMITIVE, r.getObject(i));
		    response.setContent(odata.createSerializer(responseFormat).entity(serviceMetaData, es.getEntitySet().getEntityType(), e, EntitySerializerOptions.with().contextURL(ContextURL.with().entitySet(es.getEntitySet()).build()).build()).getContent());
		    response.setStatusCode(HttpStatusCode.CREATED.getStatusCode());
		    response.setHeader(HttpHeader.CONTENT_TYPE, responseFormat.toContentTypeString());
		    return;}
		catch (Exception ex) {
		    throw new ODataApplicationException(String.format("message: %s, query: %s", ex.getMessage(), insert), 500, Locale.US);}}
	throw new IllegalStateException("Should never get here");}

    @Override
    public void updateEntity(ODataRequest request, ODataResponse response, UriInfo uriInfo, ContentType requestFormat, ContentType responseFormat) throws ODataApplicationException, DeserializerException {
	for (UriResource part : uriInfo.getUriResourceParts())
	    if (part.getKind()==UriResourceKind.entitySet) {
		UriResourceEntitySet es = (UriResourceEntitySet)part;
		Entity e = odata.createDeserializer(requestFormat).entity(request.getBody(), es.getEntityType()).getEntity();
		Map<String, String> pairs = new HashMap<>();
		for (Property p : e.getProperties()) pairs.put(p.getName(), ""+p.getValue());
		List<String> sqlPredicates = new ArrayList<>();
		for (UriParameter p : es.getKeyPredicates()) sqlPredicates.add(String.format("%s=%s", p.getName(), String.format("'%s'", p.getText())));
		String update = String.format("update %s set %s where true and %s", es.getEntitySet().getName(), pairs.toString().replace("}","").replace("{",""), String.join("and", sqlPredicates));
		try (Connection c = ds.getConnection();
		     Statement s = c.createStatement();
		     AutoCloseableWrapper<Integer> rowCount = new AutoCloseableWrapper<>(s.executeUpdate(update))) {
		    response.setStatusCode(HttpStatusCode.NO_CONTENT.getStatusCode());}
		catch (Exception ex) {
		    throw new ODataApplicationException(String.format("message: %s, query: %s", ex.getMessage(), update), 500, Locale.US);}}
	throw new IllegalStateException("Should never get here");}

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
