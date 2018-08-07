package org.protonchamber;

import java.io.*;
import java.sql.*;
import java.util.*;
import javax.naming.*;
import javax.servlet.*;
import javax.servlet.http.*;
import javax.sql.*;
import org.apache.olingo.commons.api.edmx.*;
import org.apache.olingo.server.api.*;
import org.apache.olingo.server.api.debug.*;

public class ProtonServlet extends HttpServlet {

    // class data
    
    private static final long serialVersionUID = 1L;

    // instance data

    DataSource ds;
    OData odata = OData.newInstance();
    ODataHttpHandler handler;
    String catalog, schemaPattern, tableNamePattern, columnNamePattern;

    // external API

    @Override
    public void init (ServletConfig config) throws ServletException {
	try {
	    super.init(config);
	    ds = (DataSource)((Context)(new InitialContext()) .lookup("java:comp/env")).lookup(config.getInitParameter("dsname"));
	    catalog = config.getInitParameter("CATALOG");
	    schemaPattern = config.getInitParameter("SCHEMA_PATTERN");
	    tableNamePattern = config.getInitParameter("TABLE_NAME_PATTERN");
	    columnNamePattern = config.getInitParameter("COLUMN_NAME_PATTERN");
	    handler = odata.createHandler(odata.createServiceMetadata(new ProtonEdmProvider(this, ds, catalog, schemaPattern, tableNamePattern, columnNamePattern), new ArrayList<EdmxReference>()));
	    handler.register(new ProtonEntityCollectionProcessor(ds, this));
	    handler.register(new ProtonEntityProcessor(ds, this));
	    handler.register(new DefaultDebugSupport());}
	catch (Exception e) {throw new ServletException(e);}}
	    
    @Override
    protected void service (final HttpServletRequest req, final HttpServletResponse resp) throws ServletException, IOException {
	handler.process(req, resp);}}

