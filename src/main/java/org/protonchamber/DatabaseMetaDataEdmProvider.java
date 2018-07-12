package org.protonchamber;

import java.lang.reflect.*;
import java.sql.*;
import java.util.*;
import javax.naming.*;
import javax.servlet.*;
import javax.sql.*;
import org.apache.olingo.commons.api.edm.*;
import org.apache.olingo.commons.api.edm.provider.*;
import org.apache.olingo.commons.api.ex.*;

public class DatabaseMetaDataEdmProvider extends CsdlAbstractEdmProvider {
    protected DatabaseMetaData m;
    protected GenericServlet s;

    static Map<Integer, EdmPrimitiveTypeKind> types = new HashMap<>();

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

    interface Processor {
	default void process (ResultSet r) throws SQLException {}}

    static class ProtonRoot implements Processor {
	Map<String, ProtonSchema> schemas = new HashMap<>();
	public ProtonRoot () {}
	@Override
	public void process (ResultSet r) throws SQLException {
	    schemas.putIfAbsent(r.getString("TABLE_SCHEM"), new ProtonSchema(this, r));
	    for (Processor p : schemas.values()) p.process(r);}
	public List<CsdlSchema> getSchemas () {
	    return new ArrayList<>(schemas.values());}}

    static class ProtonSchema extends CsdlSchema implements Processor {
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
	public List<CsdlEntityType> getEntityTypes () {
	    return new ArrayList<>(entityTypes.values());}
	@Override
	public CsdlEntityContainer getEntityContainer () {
	    for (CsdlEntityContainer e : entityContainers.values()) return e;
	    throw new IllegalStateException();}}

    static class ProtonEntityType extends CsdlEntityType implements Processor {
	ProtonSchema schema;
	Map<String, ProtonProperty> properties = new HashMap<>();
	public ProtonEntityType (ProtonSchema schema, ResultSet r) throws SQLException {
	    super();
	    this.schema = schema;
	    setName(r.getString("TABLE_NAME"));}
	public ProtonSchema getSchema () {
	    return schema;}
	@Override
	public void process (ResultSet r) throws SQLException {
	    if (r.getString("TABLE_SCHEM").equals(getSchema().getNamespace())) if (r.getString("TABLE_NAME").equals(getName())) properties.putIfAbsent(r.getString("COLUMN_NAME"), new ProtonProperty(this, r));
	    for (Processor p : properties.values()) p.process(r);}
	@Override
	public List<CsdlProperty> getProperties () {
	    return new ArrayList<>(properties.values());}}

    static class ProtonProperty extends CsdlProperty implements Processor {
	ProtonEntityType entityType;
	public ProtonProperty (ProtonEntityType entityType, ResultSet r) throws SQLException {
	    super();
	    this.entityType = entityType;
	    setName(r.getString("COLUMN_NAME"));
	    setDefaultValue(r.getString("COLUMN_DEF"));
	    // try {setType(types.get(r.getInt("DATA_TYPE")).getFullQualifiedName());}
	    try {setType(types.get(Types.VARCHAR).getFullQualifiedName());}
	    catch (Exception e) {setType(EdmPrimitiveTypeKind.String.getFullQualifiedName());}
	    if (getType().equals(EdmPrimitiveTypeKind.String.getFullQualifiedName())) setMaxLength(r.getInt("COLUMN_SIZE"));
	    if (r.getInt("NULLABLE")==0) setNullable(false);
	    if (r.getInt("NULLABLE")==1) setNullable(true);
	    setPrecision(r.getInt("COLUMN_SIZE"));
	    setScale(r.getInt("DECIMAL_DIGITS"));}}
	    
    static class ProtonEntityContainer extends CsdlEntityContainer implements Processor {
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
	public List<CsdlEntitySet> getEntitySets () {
	    return new ArrayList<>(entitySets.values());}}

    static class ProtonEntitySet extends CsdlEntitySet implements Processor {
	ProtonEntityContainer entityContainer;
	public ProtonEntitySet (ProtonEntityContainer entityContainer, ResultSet r) throws SQLException {
	    super();
	    this.entityContainer = entityContainer;
	    setName(r.getString("TABLE_NAME"));
	    setType(new FullQualifiedName(r.getString("TABLE_SCHEM"), r.getString("TABLE_NAME")));}}
    
    public DatabaseMetaDataEdmProvider (GenericServlet servlet, DatabaseMetaData m) {
	super();
	this.m = m;
	this.s = servlet;}

    void log (String msg) {
	s.log(msg);}

    void log (String msg, Throwable t) {
	s.log(msg, t);}

    @Override
    public CsdlEntityType getEntityType (FullQualifiedName entityTypeName) throws ODataException {
	return getRoot().schemas.get(entityTypeName.getNamespace()).entityTypes.get(entityTypeName.getName());}

    @Override
    public CsdlEntityContainer getEntityContainer () throws ODataException {
	log("OK, I'm returning an entityContainer.  But, to whom???");
	for (CsdlSchema s : getSchemas()) return s.getEntityContainer();
	log("No EntityContainer???");
	throw new IllegalStateException("No EntityContainer???");}

    @Override
    public CsdlEntitySet getEntitySet (FullQualifiedName entityContainer, String entitySetName) throws ODataException {
	log(String.format("Returning an entitySet:  %s, %s", entityContainer + "", entitySetName));
	log("Stacktrace:", new Exception());
	return getRoot().schemas.get(entityContainer.getNamespace()).getEntityContainer().getEntitySet(entitySetName);}

    @Override
    public CsdlEntityContainerInfo getEntityContainerInfo (FullQualifiedName entityContainerName) throws ODataException {
	CsdlEntityContainerInfo info = new CsdlEntityContainerInfo();
	log(String.format("entityContainerName: %s", entityContainerName));
	info.setContainerName(new FullQualifiedName("public", "EntityContainer"));
	return info;}

    ProtonRoot getRoot () throws ODataException {
	ProtonRoot root = new ProtonRoot();
	try (ResultSet r = m.getColumns(null, null, null, null)) {
	    while (r.next()) root.process(r);
	    return root;}
	catch (Throwable e) {e.printStackTrace(System.out); throw new ODataException(e);}}

    @Override
    public List<CsdlSchema> getSchemas () throws ODataException {
	return getRoot().getSchemas();}}
