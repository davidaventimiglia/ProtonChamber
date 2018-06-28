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

    // @Override
    // public List<CsdlSchema> getSchemas () throws ODataException {
    // 	try (ResultSet r = m.getSchemas()) {
    // 	    List<CsdlSchema> schemas = new ArrayList<>();
    // 	    while (r.next()) {
    // 		CsdlSchema schema = new CsdlSchema();
    // 		schema.setNamespace(r.getString("TABLE_SCHEM"));
    // 		schemas.add(schema);}
    // 	    return schemas;}
    // 	catch (Throwable e) {e.printStackTrace(System.out); throw new ODataException(e);}}

    @Override
    public List<CsdlSchema> getSchemas () throws ODataException {
	try (ResultSet r = m.getColumns(null, null, null, null)) {
	    Map<FullQualifiedName, CsdlSchema> schemas = new HashMap<>();
	    Map<FullQualifiedName, CsdlEntityType> entityTypes = new HashMap<>();
	    while (r.next()) {
		schemas.putIfAbsent(new FullQualifiedName(r.getString("TABLE_SCHEM"), null), new CsdlSchema());
		entityTypes.putIfAbsent(new FullQualifiedName(r.getString("TABLE_SCHEM"), r.getString("TABLE_NAME")), new CsdlEntityType());
		schemas.get(new FullQualifiedName(r.getString("TABLE_SCHEM"), null)).setNamespace(r.getString("TABLE_SCHEM"));
		entityTypes.get(new FullQualifiedName(r.getString("TABLE_SCHEM"), r.getString("TABLE_NAME"))).setName(r.getString("TABLE_NAME"));}
	    return new ArrayList<>(schemas.values());}
	catch (Throwable e) {e.printStackTrace(System.out); throw new ODataException(e);}}
}
