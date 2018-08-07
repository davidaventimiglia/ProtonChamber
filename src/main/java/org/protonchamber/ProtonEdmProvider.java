package org.protonchamber;

import java.lang.reflect.*;
import java.sql.*;
import java.util.*;
import javax.naming.*;
import javax.servlet.*;
import javax.sql.*;
import org.apache.olingo.commons.api.edm.*;
import org.apache.olingo.commons.api.edm.provider.*;
import org.apache.olingo.commons.api.edm.provider.CsdlNavigationProperty;
import org.apache.olingo.commons.api.edm.provider.CsdlNavigationPropertyBinding;
import org.apache.olingo.commons.api.edm.provider.annotation.*;
import org.apache.olingo.commons.api.ex.*;

public class ProtonEdmProvider extends CsdlAbstractEdmProvider {

    // nested types
    
    static interface Processor {
	default void process (ResultSet r) throws SQLException {}
	default void process (ResultSet p, boolean b) throws SQLException {}
	default void process (ResultSet x, boolean b, boolean c) throws SQLException {}}

    static interface AutoCloseableDatabaseMetaData extends AutoCloseable, DatabaseMetaData {}

    class ProtonRoot implements Processor {
	Map<String, ProtonSchema> schemas = new HashMap<>();
	public ProtonRoot () {}
	@Override
	public void process (ResultSet r) throws SQLException {
	    schemas.putIfAbsent(r.getString("TABLE_SCHEM"), new ProtonSchema(this, r));
	    for (Processor p : schemas.values()) p.process(r);}
	@Override
	public void process (ResultSet r, boolean b) throws SQLException {
	    for (Processor p : schemas.values()) p.process(r, true);}
	@Override
	public void process (ResultSet x, boolean b, boolean c) throws SQLException {
	    for (Processor p : schemas.values()) p.process(x, true, true);}
	public List<CsdlSchema> getSchemas () {
	    return new ArrayList<>(schemas.values());}}

    class ProtonSchema extends CsdlSchema implements Processor {
	ProtonRoot root;
	Map<String, ProtonEntityType> entityTypes = new HashMap<>();
	Map<String, ProtonEntityContainer> entityContainers = new HashMap<>();
	public ProtonSchema (ProtonRoot root, ResultSet r) throws SQLException {
	    super();
	    this.root = root;
	    setNamespace(r.getString("TABLE_SCHEM"));}
	@Override
	public void process (ResultSet r) throws SQLException {
	    if (r.getString("TABLE_SCHEM").equals(getNamespace())) entityTypes.putIfAbsent(r.getString("TABLE_NAME"), new ProtonEntityType(this, r));
	    if (r.getString("TABLE_SCHEM").equals(getNamespace())) entityContainers.putIfAbsent("entityContainer", new ProtonEntityContainer(this, r));
	    for (Processor p : entityTypes.values()) p.process(r);
	    for (Processor p : entityContainers.values()) p.process(r);}
	@Override
	public void process (ResultSet r, boolean b) throws SQLException {
	    if (r.getString("TABLE_SCHEM").equals(getNamespace()))
		for (Processor p : entityTypes.values()) p.process(r, true);}
	@Override
	public void process (ResultSet x, boolean b, boolean c) throws SQLException {
	    if (x.getString("PKTABLE_SCHEM").equals(getNamespace()))
		for (Processor p : entityTypes.values()) p.process(x, true, true);
	    if (x.getString("PKTABLE_SCHEM").equals(getNamespace()))
		for (Processor p : entityContainers.values()) p.process(x, true, true);}
	@Override
	public List<CsdlEntityType> getEntityTypes () {
	    return new ArrayList<>(entityTypes.values());}
	@Override
	public CsdlEntityContainer getEntityContainer () {
	    for (CsdlEntityContainer e : entityContainers.values()) return e;
	    throw new IllegalStateException();}}

    class ProtonPropertyRef extends CsdlPropertyRef {
	public ProtonPropertyRef (String alias, String name) {
	    setAlias(alias);
	    setName(name);}}

    class ProtonEntityType extends CsdlEntityType implements Processor {
	ProtonSchema schema;
	Map<String, ProtonProperty> properties = new HashMap<>();
	public ProtonEntityType (ProtonSchema schema, ResultSet r) throws SQLException {
	    super();
	    this.schema = schema;
	    setKey(new ArrayList<CsdlPropertyRef>());
	    setName(r.getString("TABLE_NAME"));}
	public ProtonSchema getSchema () {
	    return schema;}
	@Override
	public void process (ResultSet r) throws SQLException {
	    if (r.getString("TABLE_SCHEM").equals(getSchema().getNamespace())) if (r.getString("TABLE_NAME").equals(getName())) properties.putIfAbsent(r.getString("COLUMN_NAME"), new ProtonProperty(this, r));
	    for (Processor p : properties.values()) p.process(r);}
	@Override
	public void process (ResultSet r, boolean b) throws SQLException {
	    if (r.getString("TABLE_NAME").equals(getName()))
		super.getKey().add(new ProtonPropertyRef(r.getString("COLUMN_NAME"), r.getString("COLUMN_NAME")));
	    for (Processor p : properties.values()) p.process(r, true);}
	@Override
	public void process (ResultSet r, boolean b, boolean c) throws SQLException {
	    if (r.getString("PKTABLE_NAME").equals(getName()))
		getNavigationProperties().add(new ProtonNavigationProperty(this, r, true));
	    if (r.getString("FKTABLE_NAME").equals(getName()))
		getNavigationProperties().add(new ProtonNavigationProperty(this, r, false));}
	@Override
	public List<CsdlPropertyRef> getKey() {
	    if (super.getKey().isEmpty())
		for (CsdlProperty p : getProperties())
		    super.getKey().add(new ProtonPropertyRef(p.getName(), p.getName()));
	    return super.getKey();}
	@Override
	public List<CsdlProperty> getProperties () {
	    return new ArrayList<>(properties.values());}}

    class ProtonProperty extends CsdlProperty implements Processor {
	ProtonEntityType entityType;
	public ProtonProperty (ProtonEntityType entityType, ResultSet r) throws SQLException {
	    super();
	    this.entityType = entityType;
	    setName(r.getString("COLUMN_NAME"));
	    setDefaultValue(r.getString("COLUMN_DEF"));
	    try {setType(types.get(r.getInt("DATA_TYPE")).getFullQualifiedName());} catch (Exception e) {setType(EdmPrimitiveTypeKind.String.getFullQualifiedName());}
	    if (getType().equals(EdmPrimitiveTypeKind.String.getFullQualifiedName())) setMaxLength(r.getInt("COLUMN_SIZE"));
	    if (r.getInt("NULLABLE")==0) setNullable(false);
	    if (r.getInt("NULLABLE")==1) setNullable(true);
	    setPrecision(r.getInt("COLUMN_SIZE"));
	    setNullable(true);
	    setScale(r.getInt("DECIMAL_DIGITS"));}}

    class ProtonNavigationProperty extends CsdlNavigationProperty implements Processor {
	ProtonEntityType entityType;
	public ProtonNavigationProperty (ProtonEntityType entityType, ResultSet r, boolean forward) throws SQLException {
	    super();
	    this.entityType = entityType;
	    setName(r.getString(forward ? "FKTABLE_NAME" : "PKTABLE_NAME"));
	    setType(new FullQualifiedName(r.getString("PKTABLE_SCHEM"), r.getString(forward ? "FKTABLE_NAME" : "PKTABLE_NAME")).toString());
	    setCollection(forward);
	    setNullable(false);
	    CsdlAnnotation a = new CsdlAnnotation();
	    CsdlExpression e = new CsdlConstantExpression(CsdlConstantExpression.ConstantExpressionType.String, String.format("%s.%s=%s.%s", r.getString("PKTABLE_NAME"), r.getString("PKCOLUMN_NAME"), r.getString("FKTABLE_NAME"), r.getString("FKCOLUMN_NAME")));
	    a.setExpression(e);
	    a.setTerm("Core.Description");
	    getAnnotations().add(a);
	    setPartner(r.getString(forward ? "PKTABLE_NAME" : "FKTABLE_NAME"));}}

    class ProtonEntityContainer extends CsdlEntityContainer implements Processor {
	ProtonSchema schema;
	Map<String, ProtonEntitySet> entitySets = new HashMap<>();
	public ProtonEntityContainer (ProtonSchema schema, ResultSet r) throws SQLException {
	    super();
	    this.schema = schema;
	    setName("EntityContainer");}
	public ProtonSchema getSchema () {
	    return schema;}
	@Override
	public void process (ResultSet r) throws SQLException {
	    if (r.getString("TABLE_SCHEM").equals(getSchema().getNamespace())) entitySets.putIfAbsent(r.getString("TABLE_NAME"), new ProtonEntitySet(this, r));
	    for (Processor p : entitySets.values()) p.process(r);}
	@Override
	public void process (ResultSet x, boolean b, boolean c) throws SQLException {
	    for (Processor p : entitySets.values()) p.process(x, true, true);}
	@Override
	public List<CsdlEntitySet> getEntitySets () {
	    return new ArrayList<>(entitySets.values());}}

    class ProtonEntitySet extends CsdlEntitySet implements Processor {
	ProtonEntityContainer entityContainer;
	public ProtonEntitySet (ProtonEntityContainer entityContainer, ResultSet r) throws SQLException {
	    super();
	    this.entityContainer = entityContainer;
	    setName(r.getString("TABLE_NAME"));
	    setType(new FullQualifiedName(r.getString("TABLE_SCHEM"), r.getString("TABLE_NAME")));}
	@Override
	public void process (ResultSet x, boolean b, boolean c) throws SQLException {
	    if (x.getString("PKTABLE_NAME").equals(getName()))
		getNavigationPropertyBindings().add(new ProtonNavigationPropertyBinding(this, x, true));
	    if (x.getString("FKTABLE_NAME").equals(getName()))
		getNavigationPropertyBindings().add(new ProtonNavigationPropertyBinding(this, x, false));}}
    
    class ProtonNavigationPropertyBinding extends CsdlNavigationPropertyBinding implements Processor {
    	ProtonEntitySet entitySet;
    	public ProtonNavigationPropertyBinding (ProtonEntitySet entitySet, ResultSet r, boolean forward) throws SQLException {
    	    super();
    	    this.entitySet = entitySet;
    	    setPath(r.getString(forward ? "FKTABLE_NAME" : "PKTABLE_NAME"));
    	    setTarget(r.getString(forward ? "FKTABLE_NAME" : "PKTABLE_NAME"));}}

    // class data

    static Map<Integer, EdmPrimitiveTypeKind> types = new HashMap<>();

    // For Reference
    // =========================================================================
    // Binary           byte[], Byte[]
    // Boolean          Boolean
    // Byte             Short, Byte, Integer, Long, BigInteger
    // Date             Calendar, Date, Timestamp, Time, Long
    // DateTimeOffset   Timestamp, Calendar, Date, Time, Long
    // Decimal          BigDecimal, BigInteger, Double, Float, Byte, Short, Integer, Long
    // Double           Double, Float, BigDecimal, Byte, Short, Integer, Long
    // Duration         BigDecimal, BigInteger, Double, Float, Byte, Short, Integer, Long
    // Guid             UUID
    // Int16            Short, Byte, Integer, Long, BigInteger
    // Int32            Integer, Byte, Short, Long, BigInteger
    // Int64            Long, Byte, Short, Integer, BigInteger
    // SByte            Byte, Short, Integer, Long, BigInteger
    // Single           Float, Double, BigDecimal, Byte, Short, Integer, Long
    // String           String
    // TimeOfDay        Calendar, Date, Timestamp, Time, Long

    // static {types.put(Types.ARRAY, EdmPrimitiveTypeKind.);}
    static {types.put(Types.BIGINT, EdmPrimitiveTypeKind.Int64);}
    static {types.put(Types.BINARY, EdmPrimitiveTypeKind.Binary);}
    static {types.put(Types.BIT, EdmPrimitiveTypeKind.Boolean);}
    static {types.put(Types.BLOB, EdmPrimitiveTypeKind.Binary);}
    static {types.put(Types.BOOLEAN, EdmPrimitiveTypeKind.Boolean);}
    static {types.put(Types.CHAR, EdmPrimitiveTypeKind.String);}
    static {types.put(Types.CLOB, EdmPrimitiveTypeKind.String);}
    // static {types.put(Types.DATALINK, EdmPrimitiveTypeKind.);}
    static {types.put(Types.DATE, EdmPrimitiveTypeKind.Date);}
    static {types.put(Types.DECIMAL, EdmPrimitiveTypeKind.Decimal);}
    // static {types.put(Types.DISTINCT, EdmPrimitiveTypeKind.);}
    static {types.put(Types.DOUBLE, EdmPrimitiveTypeKind.Double);}
    static {types.put(Types.FLOAT, EdmPrimitiveTypeKind.Double);}
    static {types.put(Types.INTEGER, EdmPrimitiveTypeKind.Int32);}
    // static {types.put(Types.JAVA_OBJECT, EdmPrimitiveTypeKind.);}
    static {types.put(Types.LONGNVARCHAR, EdmPrimitiveTypeKind.String);}
    static {types.put(Types.LONGVARBINARY, EdmPrimitiveTypeKind.Binary);}
    static {types.put(Types.LONGVARCHAR, EdmPrimitiveTypeKind.String);}
    static {types.put(Types.NCHAR, EdmPrimitiveTypeKind.String);}
    static {types.put(Types.NCLOB, EdmPrimitiveTypeKind.String);}
    // static {types.put(Types.NULL, EdmPrimitiveTypeKind.);}
    static {types.put(Types.NUMERIC, EdmPrimitiveTypeKind.Double);}
    static {types.put(Types.NVARCHAR, EdmPrimitiveTypeKind.String);}
    // static {types.put(Types.OTHER, EdmPrimitiveTypeKind.);}
    static {types.put(Types.REAL, EdmPrimitiveTypeKind.Double);}
    // static {types.put(Types.REF, EdmPrimitiveTypeKind.);}
    // static {types.put(Types.REF_CURSOR, EdmPrimitiveTypeKind.);}
    // static {types.put(Types.ROWID, EdmPrimitiveTypeKind.);}
    static {types.put(Types.SMALLINT, EdmPrimitiveTypeKind.Int16);}
    static {types.put(Types.SQLXML, EdmPrimitiveTypeKind.String);}
    // static {types.put(Types.STRUCT, EdmPrimitiveTypeKind.);}
    static {types.put(Types.TIME, EdmPrimitiveTypeKind.TimeOfDay);}
    static {types.put(Types.TIMESTAMP, EdmPrimitiveTypeKind.Date);}
    static {types.put(Types.TIMESTAMP_WITH_TIMEZONE, EdmPrimitiveTypeKind.DateTimeOffset);}
    static {types.put(Types.TIME_WITH_TIMEZONE, EdmPrimitiveTypeKind.DateTimeOffset);}
    static {types.put(Types.TINYINT, EdmPrimitiveTypeKind.SByte);}
    static {types.put(Types.VARBINARY, EdmPrimitiveTypeKind.Binary);}
    static {types.put(Types.VARCHAR, EdmPrimitiveTypeKind.String);}

    // instance data

    DataSource ds;
    GenericServlet servlet;
    String catalog, schemaPattern, tableNamePattern, columnNamePattern;

    // internal functions

    AutoCloseableDatabaseMetaData closeable (final DatabaseMetaData m) {
	return
	    (AutoCloseableDatabaseMetaData)
	    Proxy
	    .newProxyInstance(AutoCloseableDatabaseMetaData
			      .class
			      .getClassLoader(),
			      new Class[]{AutoCloseableDatabaseMetaData.class},
			      new InvocationHandler () {
				  @Override public Object invoke (Object proxy, Method method, Object[] args) throws Throwable {
				      if ("close".equals(method.getName())) return null;
				      return method.invoke(m, args);}});}

    ProtonRoot getRoot () throws ODataException {
	servlet.log("getRoot()");
	ProtonRoot root = new ProtonRoot();
	try (Connection c = ds.getConnection();
	     AutoCloseableDatabaseMetaData m = closeable(c.getMetaData());
	     ResultSet r = m.getColumns(catalog, schemaPattern, tableNamePattern, columnNamePattern);
	     ResultSet p = m.getPrimaryKeys(catalog, schemaPattern, tableNamePattern);
	     ResultSet x = m.getCrossReference(catalog, schemaPattern, tableNamePattern, catalog, schemaPattern, tableNamePattern)) {
	    while (r.next()) root.process(r);
	    while (p.next()) root.process(p, true);
	    while (x.next()) root.process(x, true, true);
	    return root;}
	catch (Throwable e) {e.printStackTrace(System.out); throw new ODataException(e);}}

    // external API

    public ProtonEdmProvider (GenericServlet servlet, DataSource ds, String catalog, String schemaPattern, String tableNamePattern, String columnNamePattern) {
	super();
	this.ds = ds;
	this.servlet = servlet;
	this.catalog = catalog;
	this.schemaPattern = schemaPattern;
	this.tableNamePattern = tableNamePattern;
	this.columnNamePattern = columnNamePattern;}

    @Override
    public CsdlEntityType getEntityType (FullQualifiedName entityTypeName) throws ODataException {
	return getRoot().schemas.get(entityTypeName.getNamespace()).entityTypes.get(entityTypeName.getName());}

    @Override
    public CsdlEntityContainer getEntityContainer () throws ODataException {
	for (CsdlSchema s : getSchemas()) return s.getEntityContainer();
	throw new IllegalStateException("No EntityContainer???");}

    @Override
    public CsdlEntitySet getEntitySet (FullQualifiedName entityContainer, String entitySetName) throws ODataException {
	return getRoot().schemas.get(entityContainer.getNamespace()).getEntityContainer().getEntitySet(entitySetName);}

    @Override
    public CsdlEntityContainerInfo getEntityContainerInfo (FullQualifiedName entityContainerName) throws ODataException {
	CsdlEntityContainerInfo info = new CsdlEntityContainerInfo();
	info.setContainerName(new FullQualifiedName("public", "EntityContainer"));
	return info;}

    @Override
    public List<CsdlSchema> getSchemas () throws ODataException {
	return getRoot().getSchemas();}}
