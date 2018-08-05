package org.protonchamber;

import java.io.*;
import java.sql.*;
import java.util.*;
import javax.servlet.*;
import javax.sql.*;
import org.apache.olingo.commons.api.data.*;
import org.apache.olingo.commons.api.edm.*;
import org.apache.olingo.commons.api.edm.EdmEntitySet;
import org.apache.olingo.commons.api.edm.constants.*;
import org.apache.olingo.commons.api.format.*;
import org.apache.olingo.commons.api.http.*;
import org.apache.olingo.server.api.*;
import org.apache.olingo.server.api.processor.*;
import org.apache.olingo.server.api.serializer.*;
import org.apache.olingo.server.api.uri.*;
import org.apache.olingo.server.api.uri.UriResourceNavigation;

public class ProtonEntityCollectionProcessor implements EntityCollectionProcessor {

    // instance data

    OData odata;
    ServiceMetadata serviceMetaData;
    DataSource ds;
    GenericServlet servlet;

    // external API

    public ProtonEntityCollectionProcessor (DataSource ds, GenericServlet servlet) {
	this.ds = ds;
	this.servlet = servlet;}

    @Override
    public void init (OData odata, ServiceMetadata serviceMetaData) {
	this.odata = odata;
	this.serviceMetaData = serviceMetaData;}

    @Override
    public void readEntityCollection (ODataRequest request, ODataResponse response, UriInfo uriInfo, ContentType responseFormat) throws ODataApplicationException, ODataLibraryException {
	List<String> tables = new ArrayList<>();
	List<String> predicates = new ArrayList<>(); predicates.add("true");
	UriResource previous = null;
	UriResourceEntitySet ues = null;
	UriResourceNavigation nav = null;
	EdmEntitySet es = null;
	for (UriResource current : uriInfo.getUriResourceParts()) {
	    if (current instanceof UriResourceEntitySet) {
		nav = null;
		ues = (UriResourceEntitySet)current;
		es = ues.getEntitySet();
		for (UriParameter p : ues.getKeyPredicates()) predicates.add(String.format("%s.%s=%s", es.getName(), p.getName(), String.format("'%s'", p.getText())));
		tables.add(es.getName());
		previous = current;}
	    if (current instanceof UriResourceNavigation) {
		nav = (UriResourceNavigation)current;
		ues = null;
		es = (EdmEntitySet)ues.getEntitySet().getRelatedBindingTarget(nav.getProperty().getName());
		for (EdmAnnotation a : nav.getProperty().getAnnotations()) predicates.add(a.getExpression().asConstant().getValueAsString());
		tables.add(es.getName());}}
	String select = String.format("select * from %s where %s", String.join(", ", tables), String.join(" and ", predicates));
	try (Connection c = ds.getConnection();
	     Statement s = c.createStatement();
	     ResultSet r = s.executeQuery(select)) {
	    EntityCollection ec = new EntityCollection();
	    while (r.next()) {
		Entity e = new Entity();
		for (int i=1; i<=r.getMetaData().getColumnCount(); i++)
		    if (es.getEntityType().getProperty(r.getMetaData().getColumnName(i)).getType().getKind()==EdmTypeKind.PRIMITIVE)
			e.addProperty(new Property(es.getEntityType().getProperty(r.getMetaData().getColumnName(i)).getType().getName(), r.getMetaData().getColumnName(i), ValueType.PRIMITIVE, r.getObject(i)));
		ec.getEntities().add(e);}
	    response.setContent(odata.createSerializer(responseFormat).entityCollection(serviceMetaData, es.getEntityType(), ec, EntityCollectionSerializerOptions.with().id(request.getRawBaseUri() + "/" + es.getName()).contextURL(ContextURL.with().entitySet(es).build()).build()).getContent());
	    response.setStatusCode(HttpStatusCode.OK.getStatusCode());
	    response.setHeader(HttpHeader.CONTENT_TYPE, responseFormat.toContentTypeString());
	    return;}
	catch (Exception ex) {
	    throw new ODataApplicationException(String.format("message: %s, query: %s", ex.getMessage(), select), 500, Locale.US);}}}
