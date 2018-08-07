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

    public ProtonEntityProcessor (DataSource ds, GenericServlet servlet) {
	this.ds = ds;
	this.servlet = servlet;}

    @Override
    public void init (OData odata, ServiceMetadata serviceMetaData) {
	this.odata = odata;
	this.serviceMetaData = serviceMetaData;}

    @Override
    public void createEntity (ODataRequest request, ODataResponse response, UriInfo uriInfo, ContentType requestFormat, ContentType responseFormat) throws ODataApplicationException, DeserializerException, SerializerException {
	if (uriInfo.getUriResourceParts().isEmpty()) throw new IllegalStateException("No URI Resource parts!");
	if (uriInfo.getUriResourceParts().get(0).getKind()!=UriResourceKind.entitySet) throw new IllegalStateException("Not an Entity Set!");
	UriResourceEntitySet es = (UriResourceEntitySet)uriInfo.getUriResourceParts().get(0);
	List<String> names = new ArrayList<>(); List<String> values = new ArrayList<>();
	Entity e = odata.createDeserializer(requestFormat).entity(request.getBody(), es.getEntityType()).getEntity();
	for (Property p : e.getProperties()) {names.add(p.getName()); values.add(p.getValue()==null ? null
										 : String.format("'%s'", p.getValue()));}
	String insert = String.format("insert into %s (%s) values (%s)", es.getEntitySet().getName(), String.join(",", names), String.join(",", values));
	try (Connection c = ds.getConnection();
	     Statement s = c.createStatement();
	     AutoCloseableWrapper<Boolean> rowCount = new AutoCloseableWrapper<>(s.execute(insert, es.getEntityType().getPropertyNames().toArray(new String[0])));
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

    @Override
    public void updateEntity(ODataRequest request, ODataResponse response, UriInfo uriInfo, ContentType requestFormat, ContentType responseFormat) throws ODataApplicationException, DeserializerException {
	if (uriInfo.getUriResourceParts().isEmpty()) throw new IllegalStateException("No URI Resource parts!");
	if (uriInfo.getUriResourceParts().get(0).getKind()!=UriResourceKind.entitySet) throw new IllegalStateException("Not an Entity Set!");
	UriResourceEntitySet es = (UriResourceEntitySet)uriInfo.getUriResourceParts().get(0);
	List<String> names = new ArrayList<>(); List<String> values = new ArrayList<>();
	Entity e = odata.createDeserializer(requestFormat).entity(request.getBody(), es.getEntityType()).getEntity();
	Map<String, String> pairs = new HashMap<>();
	for (Property p : e.getProperties()) pairs.put(p.getName(), ""+p.getValue());
	List<String> sqlPredicates = new ArrayList<>();
	for (UriParameter p : es.getKeyPredicates()) sqlPredicates.add(String.format("%s=%s", p.getName(), String.format("'%s'", p.getText())));
	String update = String.format("update %s set %s where true and %s", es.getEntitySet().getName(), pairs.toString().replace("}","").replace("{",""), String.join("and", sqlPredicates));
	try (Connection c = ds.getConnection();
	     Statement s = c.createStatement();
	     AutoCloseableWrapper<Boolean> rowCount = new AutoCloseableWrapper<>(s.execute(update))) {
	    response.setStatusCode(HttpStatusCode.NO_CONTENT.getStatusCode());
	    return;}
	catch (Exception ex) {
	    throw new ODataApplicationException(String.format("message: %s, query: %s", ex.getMessage(), update), 500, Locale.US);}}

    public void readEntityCollection (ODataRequest request, ODataResponse response, UriInfo uriInfo, ContentType responseFormat) throws ODataApplicationException, ODataLibraryException {
	List<String> tables = new ArrayList<>();
	List<String> predicates = new ArrayList<>(); predicates.add("true");
	EdmEntitySet es = null;
	UriResourceEntitySet ues = null;
	UriResourceNavigation nav = null;
	for (UriResource current : uriInfo.getUriResourceParts()) {
	    if (current instanceof UriResourceEntitySet) {
		ues = (UriResourceEntitySet)current;
		es = ues.getEntitySet();
		for (UriParameter p : ues.getKeyPredicates()) predicates.add(String.format("%s.%s=%s", es.getName(), p.getName(), String.format("'%s'", p.getText())));}
	    if (current instanceof UriResourceNavigation) {
		nav = (UriResourceNavigation)current;
		es = (EdmEntitySet)
		    (ues.getEntitySet()
		     .getRelatedBindingTarget(nav
					      .getProperty()
					      .getName()));
		for (UriParameter p : nav.getKeyPredicates()) predicates.add(String.format("%s.%s=%s", es.getName(), p.getName(), String.format("'%s'", p.getText())));
		for (EdmAnnotation a : nav.getProperty().getAnnotations())
		    predicates
			.add(a
			     .getExpression()
			     .asConstant()
			     .getValueAsString());}
	    tables.add(es.getName());}
	Integer skip = uriInfo.getSkipOption()!=null ? uriInfo.getSkipOption().getValue() : null;
	String limit = uriInfo.getTopOption()!=null ? String.format("limit %s", uriInfo.getTopOption().getValue()) : "limit 10";
	String count = uriInfo.getCountOption()!=null && uriInfo.getCountOption().getValue() ? String.format("select count(1) from %s where %s", String.join(", ", tables), String.join(" and ", predicates)) : "select 1";
	String select = String.format("select %s.* from %s where %s %s", es.getName(), String.join(", ", tables), String.join(" and ", predicates), limit);
	try (Connection c = ds.getConnection();
	     Statement s = c.createStatement();
	     Statement t = c.createStatement();
	     ResultSet r = s.executeQuery(select);
	     ResultSet x = t.executeQuery(count)) {
	    EntityCollection ec = new EntityCollection();
	    while (r.next()) {
		if (skip!=null && skip-->0) continue;
		Entity e = new Entity();
		for (int i=1; i<=r.getMetaData().getColumnCount(); i++) {
		    if (es
			.getEntityType()
			.getProperty(r.getMetaData().getColumnName(i))
			.getType()
			.getKind()==EdmTypeKind.PRIMITIVE)
			e.addProperty(new Property(es.getEntityType().getProperty(r.getMetaData().getColumnName(i)).getType().getName(), r.getMetaData().getColumnName(i), ValueType.PRIMITIVE, r.getObject(i)));}
		ec.getEntities().add(e);}
	    while (x.next()) ec.setCount(x.getInt(1));
	    response.setContent(odata.createSerializer(responseFormat).entityCollection(serviceMetaData, es.getEntityType(), ec, EntityCollectionSerializerOptions.with().id(request.getRawBaseUri() + "/" + es.getName()).contextURL(ContextURL.with().entitySet(es).build()).count(uriInfo.getCountOption()).build()).getContent());
	    response.setStatusCode(HttpStatusCode.OK.getStatusCode());
	    response.setHeader(HttpHeader.CONTENT_TYPE, responseFormat.toContentTypeString());
	    return;}
	catch (SQLException ex) {
	    throw new ODataApplicationException(String.format("message: %s, query: %s", ex.toString(), select), 500, Locale.US);}
	catch (Exception ex) {
	    throw ex;}}

    @Override
    public void readEntity (ODataRequest request, ODataResponse response, UriInfo uriInfo, ContentType responseFormat) throws ODataApplicationException, ODataLibraryException {
	if (uriInfo.getUriResourceParts().isEmpty()) throw new IllegalStateException("No URI Resource parts!");
	if (uriInfo.getUriResourceParts().get(0).getKind()!=UriResourceKind.entitySet) throw new IllegalStateException("Not an Entity Set!");
	if (uriInfo.getUriResourceParts().size()>1) {readEntityCollection(request, response, uriInfo, responseFormat); return;}
	UriResourceEntitySet es = (UriResourceEntitySet)uriInfo.getUriResourceParts().get(0);
	Entity e = new Entity();
	Map<String, String> pairs = new HashMap<>();
	for (Property p : e.getProperties()) pairs.put(p.getName(), ""+p.getValue());
	List<String> sqlPredicates = new ArrayList<>();
	sqlPredicates.add("true");
	for (UriParameter p : es.getKeyPredicates()) sqlPredicates.add(String.format("%s=%s", p.getName(), String.format("'%s'", p.getText())));
	String select = String.format("select * from %s where %s", es.getEntitySet().getName(), String.join(" and ", sqlPredicates));
	try (Connection c = ds.getConnection();
	     Statement s = c.createStatement();
	     AutoCloseableWrapper<Boolean> rowCount = new AutoCloseableWrapper<>(s.execute(select));
	     ResultSet r = s.getResultSet()) {
	    while (r.next())
		for (int i=1; i<=r.getMetaData().getColumnCount(); i++)
		    if (es.getEntityType().getProperty(r.getMetaData().getColumnName(i)).getType().getKind()==EdmTypeKind.PRIMITIVE)
			if (e.getProperty(r.getMetaData().getColumnName(i))==null)
			    e.addProperty(new Property(es.getEntityType().getProperty(r.getMetaData().getColumnName(i)).getType().getName(), r.getMetaData().getColumnName(i), ValueType.PRIMITIVE, r.getObject(i)));
			else
			    e.getProperty(r.getMetaData().getColumnName(i)).setValue(ValueType.PRIMITIVE, r.getObject(i));
	    response.setContent(odata.createSerializer(responseFormat).entity(serviceMetaData, es.getEntitySet().getEntityType(), e, EntitySerializerOptions.with().contextURL(ContextURL.with().entitySet(es.getEntitySet()).build()).build()).getContent());
	    response.setStatusCode(HttpStatusCode.OK.getStatusCode());
	    response.setHeader(HttpHeader.CONTENT_TYPE, responseFormat.toContentTypeString());
	    return;}
	catch (Exception ex) {
	    throw new ODataApplicationException(String.format("message: %s, query: %s", ex.getMessage(), select), 500, Locale.US);}}

    @Override
    public void deleteEntity (ODataRequest request, ODataResponse response, UriInfo uriInfo) throws ODataApplicationException {
	if (uriInfo.getUriResourceParts().isEmpty()) throw new IllegalStateException("No URI Resource parts!");
	if (uriInfo.getUriResourceParts().get(0).getKind()!=UriResourceKind.entitySet) throw new IllegalStateException("Not an Entity Set!");
	UriResourceEntitySet es = (UriResourceEntitySet)uriInfo.getUriResourceParts().get(0);
	List<String> sqlPredicates = new ArrayList<>();
	for (UriParameter p : es.getKeyPredicates()) sqlPredicates.add(String.format("%s=%s", p.getName(), String.format("'%s'", p.getText())));
	String delete = String.format("delete from %s where true and %s", es.getEntitySet().getName(), String.join("and", sqlPredicates));
	try (Connection c = ds.getConnection();
	     Statement s = c.createStatement();
	     AutoCloseableWrapper<Boolean> rowCount = new AutoCloseableWrapper<>(s.execute(delete))) {
	    response.setStatusCode(HttpStatusCode.NO_CONTENT.getStatusCode());
	    return;}
	catch (Exception ex) {
	    throw new ODataApplicationException(String.format("message: %s, query: %s", ex.getMessage(), delete), 500, Locale.US);}}}
