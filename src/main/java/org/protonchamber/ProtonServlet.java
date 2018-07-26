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
    private static final long serialVersionUID = 1L;

    DataSource ds;
    ServiceMetadata m;
    OData odata;
    ODataHttpHandler handler;

    @Override
    public void init (ServletConfig config) throws ServletException {
	try {
	    ds = (DataSource)((Context)(new InitialContext()) .lookup("java:comp/env")).lookup(config.getInitParameter("dsname"));
	    m = odata.createServiceMetadata(new ProtonEdmProvider(this, ds), new ArrayList<EdmxReference>());
	    odata = OData.newInstance();
	    handler = odata.createHandler(m);
	    handler.register(new ProtonEntityCollectionProcessor(ds, this));
	    handler.register(new ProtonEntityProcessor(ds, this));
	    handler.register(new DefaultDebugSupport());}
	catch (Exception e) {throw new ServletException(e);}}
	    
    @Override
    protected void service (final HttpServletRequest req, final HttpServletResponse resp) throws ServletException, IOException {
	handler.process(req, resp);}}

