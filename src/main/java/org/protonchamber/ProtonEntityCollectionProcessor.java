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
import org.apache.olingo.server.api.processor.*;
import org.apache.olingo.server.api.serializer.*;
import org.apache.olingo.server.api.uri.*;

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
	List<String> predicates = new ArrayList<>();
	predicates.add("true");
	EdmEntitySet startEdmEntitySet = null;
	EdmEntitySet 
	for (UriResource part : uriInfo.getUriResourceParts()) {
	    if (part instanceof UriResourceEntitySet) {
		UriResourceEntitySet es = (UriResourceEntitySet)part;
		if (startEdmEntitySet==null) startEdmEntitySet = es.getEntitySet();
		tables.add(es.getEntitySet().getName());
		for (UriParameter p : es.getKeyPredicates()) predicates.add(String.format("%s.%s=%s", es.getEntitySet().getName(), p.getName(), String.format("'%s'", p.getText())));}
	    if (part instanceof UriResourceNavigation) {
		UriResourceNavigation n = (UriResourceNavigation)part;
		EdmNavigationProperty edmNavigationProperty = n.getProperty();
		for (EdmAnnotation a : edmNavigationProperty.getAnnotations()) predicates.add(a.getExpression().asConstant().getValueAsString());
		String navPropName = n.getProperty().getName();
		EdmBindingTarget edmBindingTarget = startEdmEntitySet.getRelatedBindingTarget(navPropName);
		EdmEntitySet navigationTargetEntitySet = getNavigationTargetEntitySet(uriInfo);
		tables.add(navigationTargetEntitySet.getName());}}
	servlet.log(String.format("tables: %s", tables.toString()));
	servlet.log(String.format("predicates: %s:", predicates.toString()));
	UriResourceEntitySet es = (UriResourceEntitySet)uriInfo.getUriResourceParts().get(uriInfo.getUriResourceParts().size()-1);
	Entity e = new Entity();
	String select = String.format("select * from %s where %s", String.join(", ", tables), String.join(" and ", predicates));
	try (Connection c = ds.getConnection();
	     Statement s = c.createStatement();
	     ResultSet r = s.executeQuery(select)) {
	    EntityCollection ec = new EntityCollection();
	    while (r.next()) {
		for (int i=1; i<=r.getMetaData().getColumnCount(); i++)
		    if (es.getEntityType().getProperty(r.getMetaData().getColumnName(i)).getType().getKind()==EdmTypeKind.PRIMITIVE)
			if (e.getProperty(r.getMetaData().getColumnName(i))==null)
			    e.addProperty(new Property(es.getEntityType().getProperty(r.getMetaData().getColumnName(i)).getType().getName(), r.getMetaData().getColumnName(i), ValueType.PRIMITIVE, r.getObject(i)));
			else
			    e.getProperty(r.getMetaData().getColumnName(i)).setValue(ValueType.PRIMITIVE, r.getObject(i));
		ec.getEntities().add(e);}
	    response.setContent(odata.createSerializer(responseFormat).entityCollection(serviceMetaData, es.getEntityType(), ec, EntityCollectionSerializerOptions.with().id(request.getRawBaseUri() + "/" + es.getEntitySet().getName()).contextURL(ContextURL.with().entitySet(es.getEntitySet()).build()).build()).getContent());
	    response.setStatusCode(HttpStatusCode.OK.getStatusCode());
	    response.setHeader(HttpHeader.CONTENT_TYPE, responseFormat.toContentTypeString());
	    return;}
	catch (Exception ex) {
	    throw new ODataApplicationException(String.format("message: %s, query: %s", ex.getMessage(), select), 500, Locale.US);}}

    public static EdmEntitySet getNavigationTargetEntitySet(final UriInfoResource uriInfo) throws ODataApplicationException {

	EdmEntitySet entitySet;
	final List<UriResource> resourcePaths = uriInfo.getUriResourceParts();

	// First must be entity set (hence function imports are not supported here).
	if (resourcePaths.get(0) instanceof UriResourceEntitySet) {
	    entitySet = ((UriResourceEntitySet) resourcePaths.get(0)).getEntitySet();
	} else {
	    throw new ODataApplicationException("Invalid resource type.",
						HttpStatusCode.NOT_IMPLEMENTED.getStatusCode(), Locale.ROOT);
	}

	int navigationCount = 0;
	while (entitySet != null
	       && ++navigationCount < resourcePaths.size()
	       && resourcePaths.get(navigationCount) instanceof UriResourceNavigation) {
	    final UriResourceNavigation uriResourceNavigation = (UriResourceNavigation) resourcePaths.get(navigationCount);
	    final EdmBindingTarget target = entitySet.getRelatedBindingTarget(uriResourceNavigation.getProperty().getName());
	    if (target instanceof EdmEntitySet) {
		entitySet = (EdmEntitySet) target;
	    } else {
		throw new ODataApplicationException("Singletons not supported", HttpStatusCode.NOT_IMPLEMENTED.getStatusCode(),
						    Locale.ROOT);
	    }
	}

	return entitySet;
    }


    public static UriResourceNavigation getLastNavigation(final UriInfoResource uriInfo) {

	final List<UriResource> resourcePaths = uriInfo.getUriResourceParts();
	int navigationCount = 1;
	while (navigationCount < resourcePaths.size()
	       && resourcePaths.get(navigationCount) instanceof UriResourceNavigation) {
	    navigationCount++;
	}

	return (UriResourceNavigation) resourcePaths.get(--navigationCount);
    }}
