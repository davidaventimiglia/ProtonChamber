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

public class ProtonServlet extends HttpServlet {

    private static final long serialVersionUID = 1L;

    @Override
    protected void service (final HttpServletRequest req, final HttpServletResponse resp) throws ServletException, IOException {
	try {
	    Context initCtx = new InitialContext();
	    Context envCtx = (Context)initCtx.lookup("java:comp/env");
	    DataSource ds = (DataSource)envCtx.lookup("jdbc/ProtonDB");
	    Connection conn = ds.getConnection();
	    OData odata = OData.newInstance();
	    ServiceMetadata edm = odata.createServiceMetadata(new DatabaseMetaDataEdmProvider(conn.getMetaData()), new ArrayList<EdmxReference>());
	    ODataHttpHandler handler = odata.createHandler(edm);
	    // handler.register(new DemoEntityCollectionProcessor());
	    handler.process(req, resp);
	} catch (RuntimeException e) {
	    throw new ServletException(e);
	}
	catch (SQLException | NamingException e) {
	    throw new ServletException(e);}
    }
}
