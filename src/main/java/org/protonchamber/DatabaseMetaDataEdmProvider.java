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

    interface AutoCloseableDatabaseMetaData extends AutoCloseable, DatabaseMetaData {}

    AutoCloseableDatabaseMetaData closeable (final DatabaseMetaData m) {
	return
	    (AutoCloseableDatabaseMetaData)
	    Proxy
	    .newProxyInstance(AutoCloseableDatabaseMetaData
			      .class
			      .getClassLoader(),
			      new Class[]{AutoCloseableDatabaseMetaData.class},
			      new InvocationHandler () {
				  @Override
				  public Object invoke (Object proxy, Method method, Object[] args) throws Throwable {
				      if ("close".equals(method.getName())) return null;
				      return method.invoke(m, args);}});}

    class JDBCRoot {
	Map<String, JDBCSchema> schemas = new HashMap<>();
	JDBCRoot (DatabaseMetaData m, ResultSet t, ResultSet x) throws NamingException, SQLException {
	    super();
	    while (t.next()) if (m.getUserName().equals(t.getString(GRANTEE)) && "select".equalsIgnoreCase(t.getString(PRIVILEGE))) addSchema(m, t);
	    while (x.next()) if (schemas.containsKey(x.getString(PKTABLE_SCHEM))) {
		    schemas.get(x.getString(PKTABLE_SCHEM)).getEntityType(x.getString(PKTABLE_NAME)).addNavigationProperty(m, x);}}
	void addSchema (DatabaseMetaData m, ResultSet r) throws NamingException, SQLException {
	    if (!schemas.containsKey(r.getString(TABLE_SCHEM))) schemas.put(r.getString(TABLE_SCHEM), new JDBCSchema(m, r));
	    if (m.getUserName().equals(r.getString(GRANTEE)) && "select".equalsIgnoreCase(r.getString(PRIVILEGE))) schemas.get(r.getString(TABLE_SCHEM)).addEntityType(m, r);
	    schemas.get(r.getString(TABLE_SCHEM)).addEntityContainer(m, r);}}

    class JDBCSchema extends CsdlSchema {
	Map<String, JDBCEntityContainer> entityContainers = new HashMap<>();
	Map<String, JDBCEntityType> entityTypes = new HashMap<>();
	JDBCSchema (DatabaseMetaData m, ResultSet r) throws NamingException, SQLException {
	    super();
	    setNamespace(r.getString(TABLE_SCHEM));}
	@Override
	public CsdlEntityContainer getEntityContainer () {
	    for (CsdlEntityContainer e : entityContainers.values()) return e;
	    return null;}
	public List<CsdlEntityContainer> getEntityContainers () {
	    return new ArrayList<CsdlEntityContainer>(entityContainers.values());}
	@Override
	public List<CsdlEntityType> getEntityTypes () {
	    return new ArrayList<CsdlEntityType>(entityTypes.values());}
	@Override
	public JDBCEntityType getEntityType (String name) {
	    return entityTypes.get(name);}
	JDBCEntityContainer getEntityContainer (String name) {
	    return entityContainers.get(name);}
	void addComplexTypes () {}
	void addEntityContainer (DatabaseMetaData m, ResultSet r) throws NamingException, SQLException {
	    if (!entityContainers.containsKey(r.getString(TABLE_SCHEM))) entityContainers.put(r.getString(TABLE_SCHEM), new JDBCEntityContainer(m, r));
	    if (m.getUserName().equals(r.getString(GRANTEE)) && "select".equalsIgnoreCase(r.getString(PRIVILEGE))) entityContainers.get(r.getString(TABLE_SCHEM)).addEntitySet(m, r);}
	void addEntityType (DatabaseMetaData m, ResultSet r) throws NamingException, SQLException {
	    if (!entityTypes.containsKey(r.getString(TABLE_NAME))) entityTypes.put(r.getString(TABLE_NAME), new JDBCEntityType(m, r));}}

    class JDBCNavigationProperty extends CsdlNavigationProperty {
	JDBCNavigationProperty (DatabaseMetaData m, ResultSet r) throws SQLException {
	    super();
	    // setFromRole(r.getString(PKTABLE_NAME));
	    setName(r.getString(FKTABLE_NAME));}}
	    // setRelationship(new FullQualifiedName(r.getString(PKTABLE_SCHEM), r.getString(FK_NAME)));
	    // setToRole(r.getString(FKTABLE_NAME));}}

    // class JDBCReferentialConstraint extends CsdlReferentialConstraint {
    // 	JDBCReferentialConstraint (DatabaseMetaData m, ResultSet r, AssociationEnd end1, AssociationEnd end2) throws SQLException {
    // 	    super();
    // 	    setDependent(new JDBCReferentialConstraintRole(m, r, end2, false));
    // 	    setPrincipal(new JDBCReferentialConstraintRole(m, r, end1, true));}}

    // class JDBCReferentialConstraintRole extends CsdlReferentialConstraintRole {
    // 	JDBCReferentialConstraintRole (DatabaseMetaData m, ResultSet r, AssociationEnd end, boolean end1) throws SQLException {
    // 	    super();
    // 	    setRole(end.getRole());
    // 	    setPropertyRefs(new ArrayList<PropertyRef>());
    // 	    getPropertyRefs().add(new JDBCPropertyRef(m, r, end1));}}

    class JDBCPropertyRef extends CsdlPropertyRef {
	JDBCPropertyRef (DatabaseMetaData m, ResultSet r, boolean end1) throws SQLException {
	    super();
	    setName(end1 ? r.getString(PKCOLUMN_NAME) : r.getString(FKCOLUMN_NAME));}}

    // class JDBCAssociationEnd extends CsdlAssociationEnd {
    // 	JDBCAssociationEnd (DatabaseMetaData m, ResultSet r, boolean end1) throws SQLException {
    // 	    super();
    // 	    setRole(end1 ? r.getString(PKTABLE_NAME) : r.getString(FKTABLE_NAME));
    // 	    setMultiplicity(end1 ? EdmMultiplicity.ONE : EdmMultiplicity.MANY);
    // 	    setType(end1 ? new FullQualifiedName(r.getString(PKTABLE_SCHEM), r.getString(PKTABLE_NAME)) : new FullQualifiedName(r.getString(FKTABLE_SCHEM), r.getString(FKTABLE_NAME)));
    // 	    setOnDelete(new JDBCOnDelete(m, r, end1));}}

    // class JDBCOnDelete extends CsdlOnDelete {
    // 	JDBCOnDelete (DatabaseMetaData m, ResultSet r, boolean end1) throws SQLException {
    // 	    super();
    // 	    setAction(end1 && DatabaseMetaData.importedKeyCascade==r.getShort(DELETE_RULE) ? EdmAction.Cascade : EdmAction.None);}}

    // class JDBCAnnotationAttribute extends CsdlAnnotationAttribute {}

    // class JDBCAnnotationElement extends CsdlAnnotationElement {}

    // class JDBCComplexType extends CsdlComplexType {}

    // class JDBCUsing extends CsdlUsing {}

    class JDBCEntityType extends CsdlEntityType {
	Map<String, CsdlNavigationProperty> navigationProperties = new HashMap<>();
	void addNavigationProperty (DatabaseMetaData m, ResultSet r) throws NamingException, SQLException {
	    if (!navigationProperties.containsKey(r.getString(FK_NAME))) navigationProperties.put(r.getString(FK_NAME), new JDBCNavigationProperty(m, r));}
	JDBCEntityType (DatabaseMetaData m, ResultSet r) throws NamingException, SQLException {
	    super();
	    setName(r.getString(TABLE_NAME));
	    setProperties(makeProperties(m, r.getString(TABLE_SCHEM), r.getString(TABLE_NAME)));
	    setNavigationProperties(new ArrayList<CsdlNavigationProperty>());
	    setKey(makeKey(m, r.getString(TABLE_SCHEM), r.getString(TABLE_NAME)));}
	@Override
	public List<CsdlNavigationProperty> getNavigationProperties () {
	    return new ArrayList<CsdlNavigationProperty>(navigationProperties.values());}}

    class JDBCEntityContainer extends CsdlEntityContainer {
	Map<String, JDBCEntitySet> entitySets = new HashMap<>();
	JDBCEntityContainer (DatabaseMetaData m, ResultSet r) throws NamingException, SQLException {
	    super();
	    setName(r.getString(TABLE_SCHEM));}
	@Override
	public List<CsdlEntitySet> getEntitySets () {
	    return new ArrayList<CsdlEntitySet>(entitySets.values());}
	@Override
	public CsdlEntitySet getEntitySet (String name) {
	    return entitySets.get(name);}
	void addEntitySet (DatabaseMetaData m, ResultSet r) throws NamingException, SQLException {
	    if (!entitySets.containsKey(r.getString(TABLE_NAME))) entitySets.put(r.getString(TABLE_NAME), new JDBCEntitySet(m, r));}}

    class JDBCEntitySet extends CsdlEntitySet {
	JDBCEntitySet (DatabaseMetaData m, ResultSet r) throws NamingException, SQLException {
	    super();
	    setName(r.getString(TABLE_NAME));}}
	    // setType(new FullQualifiedName(r.getString(TABLE_SCHEM), r.getString(TABLE_NAME)));}}

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

    protected String username = null;
    protected String password = null;
    protected Properties params = null;

    public DatabaseMetaDataEdmProvider (Properties params, String username, String password) {
	super();
	this.params = params;
	this.username = username;
	this.password = password;}

    protected Connection getConn () throws NamingException, SQLException {
	return username==null && password==null ? getDataSource().getConnection() : getDataSource().getConnection(username, password);}

    protected DataSource getDataSource () throws NamingException, SQLException {
	return ((DataSource)((Context)(new InitialContext()).lookup("java:comp/env")).lookup("jdbc/AtomicDB"));}

    protected CsdlEntityType makeEntityType (DatabaseMetaData meta, String schema, String table) throws NamingException, SQLException {
	CsdlEntityType e = new CsdlEntityType();
	e.setName(table);
	e.setProperties(makeProperties(meta, schema, table));
	e.setKey(makeKey(meta, schema, table));
	e.setNavigationProperties(makeNavigationProperties(meta, schema, table));
	return e;}

    protected List<CsdlNavigationProperty> makeNavigationProperties (DatabaseMetaData meta, String schema, String table) {
	List<CsdlNavigationProperty> navigationProperties = new ArrayList<CsdlNavigationProperty>();
	return navigationProperties;}

    protected List<CsdlProperty> makeProperties (DatabaseMetaData meta, String schema, String table) throws NamingException, SQLException {
	try (ResultSet r = meta.getColumns(null, schema, table, null)) {
	    List<CsdlProperty> properties = new ArrayList<CsdlProperty>();
	    while (r.next()) properties.add(makeProperty(schema, table, r.getString("COLUMN_NAME"), r.getInt("DATA_TYPE"), r.getString("COLUMN_DEF"), new Integer(r.getString("COLUMN_SIZE")), parseYesNo(r.getString("IS_NULLABLE")), new Integer(r.getString("DECIMAL_DIGITS")), new Integer(r.getString("DECIMAL_DIGITS"))));
	    return properties;}}

    protected CsdlProperty makeProperty (String schema, String table, String columnName, int dataType, String defaultValue, Integer maxLength, Boolean nullable, Integer precision, Integer scale) {
	CsdlProperty p = new CsdlProperty();
	p.setName(columnName);
	// p.setType(getType(dataType));
	// Facets f = new Facets();
	// f.setDefaultValue(defaultValue);
	// f.setMaxLength(maxLength);
	// f.setNullable(nullable);
	// f.setPrecision(precision);
	// f.setScale(scale);
	// p.setFacets(f);
	return p;}

    protected List<CsdlPropertyRef> makeKey (DatabaseMetaData meta, String schema, String table) throws NamingException, SQLException {
	return null;}
	// Key k = new Key();
	// k.setKeys(makeKeyProperties(meta, schema, table));
	// return k;}

    protected List<CsdlPropertyRef> makeKeyProperties (DatabaseMetaData meta, String schema, String table) throws NamingException, SQLException {
	try (ResultSet r = meta.getPrimaryKeys(null, schema, table)) {
	    List<CsdlPropertyRef> keyProperties = new ArrayList<CsdlPropertyRef>();
	    while (r.next()) keyProperties.add(makeKeyProperty(schema, table, r.getString("COLUMN_NAME")));
	    if (keyProperties.isEmpty()) return makePseudoKeyProperties(meta, schema, table);
	    return keyProperties;}}

    protected List<CsdlPropertyRef> makePseudoKeyProperties (DatabaseMetaData meta, String schema, String table) throws NamingException, SQLException {
	try (ResultSet r = meta.getColumns(null, schema, table, null)) {
	    List<CsdlPropertyRef> keyProperties = new ArrayList<CsdlPropertyRef>();
	    while (r.next()) keyProperties.add(makeKeyProperty(schema, table, r.getString("COLUMN_NAME")));
	    return keyProperties;}}

    protected CsdlPropertyRef makeKeyProperty (String schema, String table, String columnName) {
	CsdlPropertyRef p = new CsdlPropertyRef();
	p.setName(columnName);
	return p;}

    protected List<CsdlComplexType> makeComplexTypes (DatabaseMetaData meta, String catalog) {return new ArrayList<CsdlComplexType>();}

    protected CsdlEntitySet makeEntitySet (String schema, String setName) throws NamingException, SQLException {
	CsdlEntitySet s = new CsdlEntitySet();
	s.setName(setName);
	// s.setEntityType(new FullQualifiedName(schema, setName));
	return s;}

    protected List<CsdlFunctionImport> makeFunctionImports () {
	return new ArrayList<CsdlFunctionImport>();}

    @Override
    public CsdlComplexType getComplexType (FullQualifiedName edmFQName) throws ODataException {
	return null;}

    // @Override
    public CsdlEntitySet getEntitySet (String entityContainer, String name) throws ODataException {
	try {return makeEntitySet(entityContainer, name);}
	catch (Throwable e) {throw new ODataException(e);}}

    // @Override
    public CsdlFunctionImport getFunctionImport (String entityContainer, String name) throws ODataException {
	return null;}

    // @Override
    // public CsdlEntityContainerInfo getEntityContainerInfo (String name) throws ODataException {
    // 	if (name==null) return new CsdlEntityContainerInfo().setName("DefaultEntityContainer").setDefaultEntityContainer(true);
    // 	return new CsdlEntityContainerInfo().setName(name);}

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
	try (Connection c = getConn();
	     AutoCloseableDatabaseMetaData m = closeable(c.getMetaData());
	     ResultSet t = m.getTablePrivileges(null, null, null);
	     ResultSet x = m.getCrossReference(null, null, null, null, null, null)) {
	    return new ArrayList<CsdlSchema>((new JDBCRoot(m, t, x).schemas.values()));}
	catch (Throwable e) {throw new ODataException(e);}}}
