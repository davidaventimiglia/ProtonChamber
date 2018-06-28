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

    interface Processor {
	void process (ResultSet r) throws SQLException;}

    static class ProtonRoot implements Processor {
	Map<String, ProtonSchema> schemas = new HashMap<>();
	public ProtonRoot () {}
	@Override
	public void process (ResultSet r) throws SQLException {
	    schemas.putIfAbsent(r.getString("TABLE_SCHEM"), new ProtonSchema(r));
	    for (Processor p : schemas.values()) p.process(r);}
	public List<CsdlSchema> getSchemas () {
	    return new ArrayList<>(schemas.values());}}

    static class ProtonSchema extends CsdlSchema implements Processor {
	Map<String, ProtonEntityType> entityTypes = new HashMap<>();
	ProtonEntityContainer entityContainer = new ProtonEntityContainer();
	public ProtonSchema (ResultSet r) throws SQLException {
	    super();
	    setNamespace(r.getString("TABLE_SCHEM"));}
	@Override
	public void process (ResultSet r) throws SQLException {
	    entityTypes.putIfAbsent(r.getString("TABLE_NAME"), new ProtonEntityType(r));
	    for (Processor p : entityTypes.values()) p.process(r);
	    entityContainer.process(r);}
	@Override
	public List<CsdlEntityType> getEntityTypes () {
	    return new ArrayList<>(entityTypes.values());}
	@Override
	public CsdlEntityContainer getEntityContainer () {
	    return entityContainer;}}

    static class ProtonEntityType extends CsdlEntityType implements Processor {
	public ProtonEntityType (ResultSet r) throws SQLException {
	    super();
	    setName(r.getString("TABLE_NAME"));}
	@Override
	public void process (ResultSet r) {}}

    static class ProtonEntityContainer extends CsdlEntityContainer implements Processor {
	Map<String, ProtonEntitySet> entitySets = new HashMap<>();
	public ProtonEntityContainer (ResultSet r) throws SQLException {
	    super();
	    setName("EntityContainer");}
	@Override
	public void process (ResultSet r) {
	    entitySets.putIfAbsent(r.getString("TABLE_NAME"), new ProtonEntitySet(r));
	    for (Processor p : entitySets.values()) p.process(r);}
	@Override
	public List<CsdlEntitySet> getEntitySets () {
	    return new ArrayList<>(entitySets.values());}}

    static class ProtonEntitySet extends CsdlEntitySet implements Processor {
	public ProtonEntitySet (ResultSet r) throws SQLException {
	    super();
	    setName(r.getString("TABLE_NAME"));}
	@Override
	public void process (ResultSet r) {}}

    static class ProtonEntitySet extends CsdlEntitySet implements Processor {
	public ProtonEntitySet (ResultSet r) throws SQLException {
	    super();
	    setName(r.getString("TABLE_NAME"));}
	@Override
	public void process (ResultSet r) {}}
    
    public DatabaseMetaDataEdmProvider (DatabaseMetaData m) {
	super();
	this.m = m;}

    @Override
    public List<CsdlSchema> getSchemas () throws ODataException {
	ProtonRoot root = new ProtonRoot();
	try (ResultSet r = m.getColumns(null, null, null, null)) {
	    while (r.next()) root.process(r);
	    return root.getSchemas();}
	catch (Throwable e) {e.printStackTrace(System.out); throw new ODataException(e);}}
}
