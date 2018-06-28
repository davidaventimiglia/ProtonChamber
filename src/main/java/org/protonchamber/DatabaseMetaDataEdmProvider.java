package org.protonchamber;

import java.lang.reflect.*;
import java.sql.*;
import java.util.*;
import javax.naming.*;
import javax.sql.*;
import org.apache.olingo.commons.api.edm.*;
import org.apache.olingo.commons.api.edm.provider.*;
import org.apache.olingo.commons.api.ex.*;

public class DatabaseMetaDataEdmProvider extends CsdlAbstractEdmProvider {
    protected DatabaseMetaData m;

    public DatabaseMetaDataEdmProvider (DatabaseMetaData m) {
	super();
	this.m = m;}

    @Override
    public List<CsdlSchema> getSchemas () throws ODataException {
	try (ResultSet r = m.getSchemas()) {
	    List<CsdlSchema> schemas = new ArrayList<>();
	    while (r.next()) {
		CsdlSchema schema = new CsdlSchema();
		schema.setNamespace(r.getString("TABLE_SCHEM"));
		schemas.add(schema);}
	    return schemas;}
	catch (Throwable e) {e.printStackTrace(System.out); throw new ODataException(e);}}
}
