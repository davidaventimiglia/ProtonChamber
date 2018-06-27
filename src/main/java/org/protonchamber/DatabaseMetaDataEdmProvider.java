package org.protonchamger;

import java.lang.reflect.*;
import java.sql.*;
import java.util.*;
import javax.naming.*;
import javax.sql.*;
import org.apache.olingo.commons.api.edm.*;
import org.apache.olingo.commons.api.edm.provider.*;
import org.apache.olingo.commons.api.ex.*;

public class DatabaseMetaDataEdmProvider extends CsdlAbstractEdmProvider {

    class ProtonEdmDataModel {
	Map<String, ProtonSchema> schemas = new HashMap<>();
	ProtonEdmDataModel (DatabaseMetaData m, ResultSet t, ResultSet x) throws NamingException, SQLException {
	    while (t.next()) addSchema(m, t);}
	// while (x.next()) if (schemas.containsKey(x.getString(PKTABLE_SCHEM))) schemas.get(x.getString(PKTABLE_SCHEM)).getEntityType(x.getString(PKTABLE_NAME)).addNavigationProperty(m, x);}
	void addSchema (DatabaseMetaData m, ResultSet r) throws NamingException, SQLException {
	    schemas.putIfAbsent(r.getString(TABLE_SCHEM), new ProtonSchema(m, r));
	    schemas.get(r.getString(TABLE_SCHEM)).addEntityType(m, r);
	    schemas.get(r.getString(TABLE_SCHEM)).addEntityContainer(m, r);}}

    class ProtonSchema extends CsdlSchema {
	Map<String, ProtonEntityContainer> entityContainers = new HashMap<>();
	Map<String, ProtonEntityType> entityTypes = new HashMap<>();
	ProtonSchema (DatabaseMetaData m, ResultSet r) throws NamingException, SQLException {
	    super();
	    setNamespace(r.getString(TABLE_SCHEM));}
	@Override public CsdlEntityContainer getEntityContainer () {
	    for (CsdlEntityContainer e : entityContainers.values()) return e;
	    return null;}
	@Override public List<CsdlEntityType> getEntityTypes () {
	    return new ArrayList<CsdlEntityType>(entityTypes.values());}
	@Override public ProtonEntityType getEntityType (String name) {
	    return entityTypes.get(name);}
	ProtonEntityContainer getEntityContainer (String name) {
	    return entityContainers.get(name);}
	void addComplexTypes () {}
	void addEntityContainer (DatabaseMetaData m, ResultSet r) throws NamingException, SQLException {
	    if (!entityContainers.containsKey(r.getString(TABLE_SCHEM))) entityContainers.put(r.getString(TABLE_SCHEM), new ProtonEntityContainer(m, r));
	    entityContainers.get(r.getString(TABLE_SCHEM)).addEntitySet(m, r);}
	void addEntityType (DatabaseMetaData m, ResultSet r) throws NamingException, SQLException {
	    if (!entityTypes.containsKey(r.getString(TABLE_NAME))) entityTypes.put(r.getString(TABLE_NAME), new ProtonEntityType(m, r));}}

    class ProtonEntityContainer extends CsdlEntityContainer {
	Map<String, ProtonEntitySet> entitySets = new HashMap<>();
	ProtonEntityContainer (DatabaseMetaData m, ResultSet r) throws NamingException, SQLException {
	    super();
	    setName(r.getString(TABLE_SCHEM));}
	@Override public List<CsdlEntitySet> getEntitySets () {
	    return new ArrayList<CsdlEntitySet>(entitySets.values());}
	@Override public CsdlEntitySet getEntitySet (String name) {
	    return entitySets.get(name);}
	void addEntitySet (DatabaseMetaData m, ResultSet r) throws NamingException, SQLException {
	    if (!entitySets.containsKey(r.getString(TABLE_NAME))) entitySets.put(r.getString(TABLE_NAME), new ProtonEntitySet(m, r));}}

    class ProtonEntityType extends CsdlEntityType {
	ProtonEntityType (DatabaseMetaData m, ResultSet r) throws NamingException, SQLException {
	    super();}}

    class ProtonEntitySet extends CsdlEntitySet {
	ProtonEntitySet (DatabaseMetaData m, ResultSet r) throws NamingException, SQLException {
	    super();
	    setName(r.getString(TABLE_NAME));}}

    // --------------------------------------------------------------------------------
    // Class-level definitions
    // --------------------------------------------------------------------------------

    public static String DEFERRABILITY = "DEFERRABILITY";
    public static String DELETE_RULE = "DELETE_RULE";
    public static String FKCOLUMN_NAME = "FKCOLUMN_NAME";
    public static String FKTABLE_CAT = "FKTABLE_CAT";
    public static String FKTABLE_NAME = "FKTABLE_NAME";
    public static String FKTABLE_SCHEM = "FKTABLE_SCHEM";
    public static String FK_NAME = "FK_NAME";
    public static String GRANTEE = "GRANTEE";
    public static String GRANTOR = "GRANTOR";
    public static String IS_GRANTABLE = "IS_GRANTABLE";
    public static String KEY_SEQ = "KEY_SEQ";
    public static String PKCOLUMN_NAME = "PKCOLUMN_NAME";
    public static String PKTABLE_CAT = "PKTABLE_CAT";
    public static String PKTABLE_NAME = "PKTABLE_NAME";
    public static String PKTABLE_SCHEM = "PKTABLE_SCHEM";
    public static String PK_NAME = "PK_NAME";
    public static String PRIVILEGE = "PRIVILEGE";
    public static String TABLE_CAT = "TABLE_CAT";
    public static String TABLE_NAME = "TABLE_NAME";
    public static String TABLE_SCHEM = "TABLE_SCHEM";
    public static String UPDATE_RULE = "UPDATE_RULE";
    public static String PROVIDER = "PROVIDER";

    public static Boolean parseYesNo (String v) {
	if (v==null) return new Boolean(false);
	if (v.equalsIgnoreCase("YES")) return new Boolean(true);
	return false;}

    // --------------------------------------------------------------------------------
    // Instance-level definitions
    // --------------------------------------------------------------------------------

    protected Properties params = null;
    protected DatabaseMetaData m;

    public DatabaseMetaDataEdmProvider (DatabaseMetaData m) {
	super();
	this.m = m;}

    protected List<CsdlProperty> makeProperties (DatabaseMetaData meta, String schema, String table) throws NamingException, SQLException {
	try (ResultSet r = meta.getColumns(null, schema, table, null)) {
	    List<CsdlProperty> properties = new ArrayList<CsdlProperty>();
	    while (r.next()) properties.add(makeProperty(schema, table, r.getString("COLUMN_NAME"), r.getInt("DATA_TYPE"), r.getString("COLUMN_DEF"), new Integer(r.getString("COLUMN_SIZE")), parseYesNo(r.getString("IS_NULLABLE")), new Integer(r.getString("DECIMAL_DIGITS")), new Integer(r.getString("DECIMAL_DIGITS"))));
	    return properties;}}

    protected CsdlProperty makeProperty (String schema, String table, String columnName, int dataType, String defaultValue, Integer maxLength, Boolean nullable, Integer precision, Integer scale) {
	CsdlProperty p = new CsdlProperty();
	p.setName(columnName);
	return p;}

    protected CsdlEntitySet makeEntitySet (String schema, String setName) throws NamingException, SQLException {
	CsdlEntitySet s = new CsdlEntitySet();
	s.setName(setName);
	return s;}

    @Override
    public CsdlEntitySet getEntitySet (FullQualifiedName entityContainer, String name) throws ODataException {
	try {return makeEntitySet(entityContainer.toString(), name);}
	catch (Throwable e) {throw new ODataException(e);}}

    @Override
    public CsdlEntityType getEntityType (FullQualifiedName edmFQName) throws ODataException {
	for (CsdlSchema s : getSchemas())
	    if (s.getNamespace().equals(edmFQName.getNamespace()))
		for (CsdlEntityType e : s.getEntityTypes())
		    if (e.getName().equals(edmFQName.getName()))
			return e;
	throw new ODataException(String.format("%s not found", edmFQName));}

    @Override
    public List<CsdlSchema> getSchemas () throws ODataException {
	try (ResultSet t = m.getTablePrivileges(null, null, null);
	     ResultSet x = m.getCrossReference(null, null, null, null, null, null)) {
	    return new ArrayList<CsdlSchema>((new ProtonEdmDataModel(m, t, x).schemas.values()));}
	catch (Throwable e) {throw new ODataException(e);}}}
