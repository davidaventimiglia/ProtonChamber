package org.protonchamber;

import java.io.*;
import java.lang.reflect.*;
import java.sql.*;
import java.time.*;
import java.util.*;
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

public class SQLProcessor implements EntityProcessor, EntityCollectionProcessor, PrimitiveProcessor {

    OData odata;
    ServiceMetadata serviceMetaData;

    @Override
    public void init (OData odata, ServiceMetadata serviceMetaData) {
	this.odata = odata;
	this.serviceMetaData = serviceMetaData;}

    private String getSQL (String product, String name, DB db) {
	ST st;
	STGroup g = new STGroupFile(String.format("%s.stg", product));
	if (g==null) g = new STGroupFile("default.stg");
	st = g.getInstanceOf(name);
	st.add("info", db);
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
	    this.e = this.odata.createDeserializer(requestFormat).entity(request.getBody(), getEntitySet().getEntityType()).getEntity();}

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
	response.setStatusCode(HttpStatusCode.OK.getStatusCode());
	response.addHeader("SQL", getSQL("PostgreSQL", "getEntityCollection_select", db));
	response.addHeader("SQL", getSQL("PostgreSQL", uriInfo.getCountOption()!=null && uriInfo.getCountOption().getValue() ? "getEntityCollection_count_n" : "getEntityCollection_count_1", db));
	response.setHeader(HttpHeader.CONTENT_TYPE, responseFormat.toContentTypeString());}
	
    @Override
    public void readEntity (ODataRequest request, ODataResponse response, UriInfo uriInfo, ContentType responseFormat) throws ODataApplicationException, ODataLibraryException {
	DB db = new DB(odata, request, response, uriInfo, responseFormat);
	response.setStatusCode(HttpStatusCode.OK.getStatusCode());
	response.addHeader("SQL", getSQL("PostgreSQL", "getEntityCollection_select", db));
	response.addHeader("SQL", getSQL("PostgreSQL", uriInfo.getCountOption()!=null && uriInfo.getCountOption().getValue() ? "getEntityCollection_count_n" : "getEntityCollection_count_1", new DB(odata, request, response, uriInfo, responseFormat)));
	response.setHeader(HttpHeader.CONTENT_TYPE, responseFormat.toContentTypeString());}

    @Override
    public void deleteEntity (ODataRequest request, ODataResponse response, UriInfo uriInfo) throws ODataApplicationException {
	DB db = new DB(odata, request, response, uriInfo);
	response.setStatusCode(HttpStatusCode.NO_CONTENT.getStatusCode());
	response.addHeader("SQL", getSQL("PostgreSQL", "deleteEntity", db));}

    @Override
    public void updateEntity (ODataRequest request, ODataResponse response, UriInfo uriInfo, ContentType requestFormat, ContentType responseFormat) throws ODataApplicationException, DeserializerException {
	DB db = new DB(odata, request, response, uriInfo, requestFormat, responseFormat);
	response.setStatusCode(HttpStatusCode.NO_CONTENT.getStatusCode());
	response.addHeader("SQL", getSQL("PostgreSQL", "updateEntity", db));}

    @Override
    public void createEntity (ODataRequest request, ODataResponse response, UriInfo uriInfo, ContentType requestFormat, ContentType responseFormat) throws ODataApplicationException, DeserializerException, SerializerException {
	DB db = new DB(odata, request, response, uriInfo, requestFormat, responseFormat);
	response.setStatusCode(HttpStatusCode.CREATED.getStatusCode());
	response.addHeader("SQL", getSQL("PostgreSQL", "insertEntity", db));
	response.setHeader(HttpHeader.CONTENT_TYPE, responseFormat.toContentTypeString());}

    @Override
    public void deletePrimitive (ODataRequest request, ODataResponse response, UriInfo uriInfo) throws ODataApplicationException {
	DB db = new DB(odata, request, response, uriInfo);}

    @Override
    public void readPrimitive (ODataRequest request, ODataResponse response, UriInfo uriInfo, ContentType requestFormat) throws ODataApplicationException, ODataLibraryException {
	DB db = new DB(odata, request, response, uriInfo);}

    @Override
    public void updatePrimitive (ODataRequest request, ODataResponse response, UriInfo uriInfo, ContentType requestFormat, ContentType responseFormat) throws ODataApplicationException, ODataLibraryException {
	DB db = new DB(odata, request, response, uriInfo);}}
