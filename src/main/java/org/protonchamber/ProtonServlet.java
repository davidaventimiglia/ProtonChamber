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
	try (Connection conn = ((DataSource)((Context)(new InitialContext()).lookup("java:comp/env")).lookup("jdbc/ProtonDB")).getConnection()) {
	    OData odata = OData.newInstance();
	    ServiceMetadata edm = odata.createServiceMetadata(new DatabaseMetaDataEdmProvider(conn.getMetaData()), new ArrayList<EdmxReference>());
	    ODataHttpHandler handler = odata.createHandler(edm);
	    handler.register(new ProtonEntityCollectionProcessor());
	    log("about to handle request");
	    handler.process(req, resp);
	    log("handled request");
	} catch (RuntimeException e) {
	    log(e.getMessage());
	    throw new ServletException(e);
	}
	catch (SQLException | NamingException e) {
	    throw new ServletException(e);}
    }
}
