package org.protonchamber;

import java.io.*;
import java.lang.reflect.*;
import java.sql.*;
import java.time.*;
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
import org.stringtemplate.v4.*;

public class ProtonProcessor implements EntityProcessor, EntityCollectionProcessor, PrimitiveProcessor {

    // nested types

    static class AutoCloseableWrapper<T> implements AutoCloseable {
	T wrapped;
	public AutoCloseableWrapper (T wrapped) {this.wrapped = wrapped;}
	@Override
	public void close () {}
	public T getWrapped () {return wrapped;}}

    // instance data

    OData odata;
    ServiceMetadata serviceMetaData;
    DataSource ds;
    GenericServlet servlet;

    // external API

    public ProtonProcessor (DataSource ds, GenericServlet servlet) {
	this.ds = ds;
	this.servlet = servlet;}

    @Override
    public void init (OData odata, ServiceMetadata serviceMetaData) {
	this.odata = odata;
	this.serviceMetaData = serviceMetaData;}

    private String getSQL (Connection c, String name, DB db) throws SQLException {
	ST st;
	STGroup g = new STGroupFile(String.format("%s.stg", c.getMetaData().getDatabaseProductName()));
	if (g==null) g = new STGroupFile("default.stg");
	st = g.getInstanceOf(name);
	st.add("info", db);
	servlet.log(String.format("st.render(): %s", st.render()));
	return st.render();}

    class DB {
	OData odata;
	ODataRequest request;
	ODataResponse response;
	UriInfo uriInfo;
	ContentType responseFormat;
	ContentType requestFormat;
	Entity e;

	DB (OData odata, ODataRequest request, ODataResponse response, UriInfo uriInfo) {
	    this.odata = odata;
	    this.request = request;
	    this.response = response;
	    this.uriInfo = uriInfo;}

	DB (OData odata, ODataRequest request, ODataResponse response, UriInfo uriInfo, ContentType responseFormat) {
	    this(odata, request, response, uriInfo);
	    this.responseFormat = responseFormat;}

	DB (OData odata, ODataRequest request, ODataResponse response, UriInfo uriInfo, ContentType requestFormat, ContentType responseFormat) throws DeserializerException {
	    this(odata, request, response, uriInfo, responseFormat);
	    this.e = this.odata.createDeserializer(requestFormat).entity(request.getBody(), getEntitySet().getEntityType()).getEntity();
	    servlet.log(String.format("this.e: %s", this.e));}

	public EdmEntitySet getEntitySet () {
	    EdmEntitySet es = null;
	    for (UriResource p : uriInfo.getUriResourceParts())
		if (p instanceof UriResourceEntitySet)
		    es = ((UriResourceEntitySet)p).getEntitySet();
		else if (p instanceof UriResourceNavigation)
		    es = (EdmEntitySet)
			es.getRelatedBindingTarget(((UriResourceNavigation)p)
						   .getProperty()
						   .getName());
		else throw new IllegalStateException("No EntitySet!");
	    return es;}

	public Entity getEntity () {
	    return e;}

	public EdmEntityType getEntityType () {
	    EdmEntityType es = null;
	    for (UriResource p : uriInfo.getUriResourceParts())
		if (p instanceof UriResourceEntitySet)
		    es = ((UriResourceEntitySet)p).getEntityType();
		else if (p instanceof UriResourceNavigation)
		    es = (EdmEntityType)
			es.getNavigationProperty(((UriResourceNavigation)p)
						 .getProperty()
						 .getName()).getType();
		else throw new IllegalStateException("No EntitySet!");
	    return es;}

	public List<String> getTables () {
	    ArrayList<String> tables = new ArrayList<>();
	    for (UriResource p : uriInfo.getUriResourceParts())
		tables.add(p.getSegmentValue());
	    return tables;}

	public String getTable () {
	    List<String> tables = getTables();
	    return tables.get(tables.size()-1);}

	public List<String> getColumns () {
	    return new ArrayList<>();}

	public List<String> getKeys () {
	    ArrayList<String> keys = new ArrayList<>();
	    for (UriResource p : uriInfo.getUriResourceParts())
		if (p instanceof UriResourceEntitySet)
		    for (UriParameter x : ((UriResourceEntitySet)p).getKeyPredicates())
			keys.add(String.format("%s.%s", p.getSegmentValue(), x.getName()));
		else if (p instanceof UriResourceNavigation)
		    for (UriParameter x : ((UriResourceNavigation)p).getKeyPredicates())
			keys.add(String.format("%s.%s", p.getSegmentValue(), x.getName()));
	    return keys;}

	public Set<Map.Entry<String, String>> getPairs () throws DeserializerException {
	    Map<String, String> pairs = new HashMap<>();
	    List<String> names = getEntityType().getPropertyNames();
	    for (Property p : e.getProperties())
		if (names.contains(p.getName()))
		    if (getEntityType().getProperty(p.getName()).getType().getKind()==EdmTypeKind.PRIMITIVE) pairs.put(p.getName(), String.format("'%s'", ((EdmPrimitiveType)getEntityType().getProperty(p.getName()).getType()).getDefaultType().isAssignableFrom(Calendar.class) ? ((Calendar)p.getValue()).getTime().toInstant().atZone(ZoneId.of("Africa/Tunis")).toLocalDate() : p.getValue()));
	    return pairs.entrySet();}

	public List<String> getPredicates () {
	    ArrayList<String> predicates = new ArrayList<>();
	    predicates.add("true");
	    for (UriResource p : uriInfo.getUriResourceParts())
		if (p instanceof UriResourceEntitySet)
		    for (UriParameter x : ((UriResourceEntitySet)p).getKeyPredicates())
			predicates.add(String.format("%s.%s=%s", p.getSegmentValue(), x.getName(), x.getText()));
		else if (p instanceof UriResourceNavigation)
		    for (UriParameter x : ((UriResourceNavigation)p).getKeyPredicates())
			predicates.add(String.format("%s.%s=%s", p.getSegmentValue(), x.getName(), x.getText()));
	    return predicates;}
	
	public String getLimit () {
	    return uriInfo.getTopOption()!=null ? String.format("limit %s", uriInfo.getTopOption().getValue()) : "limit 10";}

	public int getSkip () {
	    return uriInfo.getSkipOption()!=null ? uriInfo.getSkipOption().getValue() : 0;}}

    @Override
    public void readEntityCollection (ODataRequest request, ODataResponse response, UriInfo uriInfo, ContentType responseFormat) throws ODataApplicationException, ODataLibraryException {
	DB db = new DB(odata, request, response, uriInfo, responseFormat);
	try (Connection c = ds.getConnection();
	     Statement s = c.createStatement();
	     Statement t = c.createStatement();
	     ResultSet r = s.executeQuery(getSQL(c, "getEntityCollection_select", db));
	     ResultSet x = t.executeQuery(getSQL(c, uriInfo.getCountOption()!=null && uriInfo.getCountOption().getValue() ? "getEntityCollection_count_n" : "getEntityCollection_count_1", new DB(odata, request, response, uriInfo, responseFormat)))) {
	    EntityCollection ec = new EntityCollection();
	    EdmEntitySet es = db.getEntitySet();
	    int skip = db.getSkip();
	    while (r.next()) {
		if (skip-->0) continue;
		Entity e = new Entity();
		for (int i=1; i<=r.getMetaData().getColumnCount(); i++)
		    if (es.getEntityType().getProperty(r.getMetaData().getColumnName(i)).getType().getKind()==EdmTypeKind.PRIMITIVE)
			e.addProperty(new Property(es.getEntityType().getProperty(r.getMetaData().getColumnName(i)).getType().getName(), r.getMetaData().getColumnName(i), ValueType.PRIMITIVE, r.getObject(i)));
		ec.getEntities().add(e);}
	    while (x.next())
		ec.setCount(x.getInt(1));
	    response.setStatusCode(HttpStatusCode.OK.getStatusCode());
	    response.setHeader(HttpHeader.CONTENT_TYPE, responseFormat.toContentTypeString());
	    response.setContent(odata.createSerializer(responseFormat).entityCollection(serviceMetaData, es.getEntityType(), ec, EntityCollectionSerializerOptions.with().id(request.getRawBaseUri() + "/" + es.getName()).contextURL(ContextURL.with().entitySet(es).build()).count(uriInfo.getCountOption()).build()).getContent());}
	catch (SQLException ex) {
	    throw new ODataApplicationException(String.format("message: %s", ex.toString()), 500, Locale.US);}}

    @Override
    public void readEntity (ODataRequest request, ODataResponse response, UriInfo uriInfo, ContentType responseFormat) throws ODataApplicationException, ODataLibraryException {
	DB db = new DB(odata, request, response, uriInfo, responseFormat);
	try (Connection c = ds.getConnection();
	     Statement s = c.createStatement();
	     Statement t = c.createStatement();
	     ResultSet r = s.executeQuery(getSQL(c, "getEntityCollection_select", db));
	     ResultSet x = t.executeQuery(getSQL(c, uriInfo.getCountOption()!=null && uriInfo.getCountOption().getValue() ? "getEntityCollection_count_n" : "getEntityCollection_count_1", new DB(odata, request, response, uriInfo, responseFormat)))) {
	    EntityCollection ec = new EntityCollection();
	    EdmEntitySet es = db.getEntitySet();
	    int skip = db.getSkip();
	    while (r.next()) {
		if (skip-->0) continue;
		Entity e = new Entity();
		for (int i=1; i<=r.getMetaData().getColumnCount(); i++)
		    if (es.getEntityType().getProperty(r.getMetaData().getColumnName(i)).getType().getKind()==EdmTypeKind.PRIMITIVE) e.addProperty(new Property(es.getEntityType().getProperty(r.getMetaData().getColumnName(i)).getType().getName(), r.getMetaData().getColumnName(i), ValueType.PRIMITIVE, r.getObject(i)));
		ec.getEntities().add(e);}
	    while
		(x.next()) ec.setCount(x.getInt(1));
	    response.setStatusCode(HttpStatusCode.OK.getStatusCode());
	    response.setHeader(HttpHeader.CONTENT_TYPE, responseFormat.toContentTypeString());
	    for (Entity e : ec.getEntities())
		response.setContent(odata.createSerializer(responseFormat).entity(serviceMetaData, es.getEntityType(), e, EntitySerializerOptions.with().contextURL(ContextURL.with().entitySet(es).build()).build()).getContent());}
	catch (SQLException ex) {
	    throw new ODataApplicationException(String.format("message: %s", ex.toString()), 500, Locale.US);}}

    @Override
    public void deleteEntity (ODataRequest request, ODataResponse response, UriInfo uriInfo) throws ODataApplicationException {
	DB db = new DB(odata, request, response, uriInfo);
	try (Connection c = ds.getConnection();
	     Statement s = c.createStatement();
	     Statement t = c.createStatement();
    	     AutoCloseableWrapper<Boolean> rowCount = new AutoCloseableWrapper<>(s.execute(getSQL(c, "deleteEntity", db)))) {
	    response.setStatusCode(HttpStatusCode.NO_CONTENT.getStatusCode());}
	catch (SQLException ex) {
	    throw new ODataApplicationException(String.format("message: %s", ex.toString()), 500, Locale.US);}}

    @Override
    public void updateEntity(ODataRequest request, ODataResponse response, UriInfo uriInfo, ContentType requestFormat, ContentType responseFormat) throws ODataApplicationException, DeserializerException {
	DB db = new DB(odata, request, response, uriInfo, requestFormat, responseFormat);
	try (Connection c = ds.getConnection();
	     Statement s = c.createStatement();
	     Statement t = c.createStatement();
    	     AutoCloseableWrapper<Integer> rowCount = new AutoCloseableWrapper<>(s.executeUpdate(getSQL(c, "updateEntity", db)))) {
	    response.setStatusCode(HttpStatusCode.NO_CONTENT.getStatusCode());}
	catch (SQLException ex) {
	    throw new ODataApplicationException(String.format("message: %s", ex.toString()), 500, Locale.US);}}

    @Override
    public void createEntity (ODataRequest request, ODataResponse response, UriInfo uriInfo, ContentType requestFormat, ContentType responseFormat) throws ODataApplicationException, DeserializerException, SerializerException {
	DB db = new DB(odata, request, response, uriInfo, requestFormat, responseFormat);
	try (Connection c = ds.getConnection();
	     Statement s = c.createStatement();
	     Statement t = c.createStatement();
    	     AutoCloseableWrapper<Integer> rowCount = new AutoCloseableWrapper<>(s.executeUpdate(getSQL(c, "insertEntity", db), Statement.RETURN_GENERATED_KEYS));
	     ResultSet r = s.getGeneratedKeys()) {
	    while (r.next())
		for (int i=1; i<=r.getMetaData().getColumnCount(); i++)
		    if (db.getEntityType().getProperty(r.getMetaData().getColumnName(i)).getType().getKind()==EdmTypeKind.PRIMITIVE)
			if (db.getEntity().getProperty(r.getMetaData().getColumnName(i))==null)
			    db.getEntity().addProperty(new Property(db.getEntityType().getProperty(r.getMetaData().getColumnName(i)).getType().getName(), r.getMetaData().getColumnName(i), ValueType.PRIMITIVE, r.getObject(i)));
			else
			    db.getEntity().getProperty(r.getMetaData().getColumnName(i)).setValue(ValueType.PRIMITIVE, r.getObject(i));
	    response.setContent(odata.createSerializer(responseFormat).entity(serviceMetaData, db.getEntityType(), db.getEntity(), EntitySerializerOptions.with().contextURL(ContextURL.with().entitySet(db.getEntitySet()).build()).build()).getContent());
	    response.setStatusCode(HttpStatusCode.CREATED.getStatusCode());
	    response.setHeader(HttpHeader.CONTENT_TYPE, responseFormat.toContentTypeString());}
	catch (SQLException ex) {
	    throw new ODataApplicationException(String.format("message: %s", ex.toString()), 500, Locale.US);}}

    @Override
    public void deletePrimitive(ODataRequest request, ODataResponse response, UriInfo uriInfo) throws ODataApplicationException {
	DB db = new DB(odata, request, response, uriInfo);}

    @Override
    public void readPrimitive(ODataRequest request, ODataResponse response, UriInfo uriInfo, ContentType requestFormat) throws ODataApplicationException, ODataLibraryException {
	DB db = new DB(odata, request, response, uriInfo);}

    @Override
    public void updatePrimitive(ODataRequest request, ODataResponse response, UriInfo uriInfo, ContentType requestFormat, ContentType responseFormat) throws ODataApplicationException, ODataLibraryException {
	DB db = new DB(odata, request, response, uriInfo);}}
