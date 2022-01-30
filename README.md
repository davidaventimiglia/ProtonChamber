# README #

### What is this repository for? ###

This project creates an ODATA server for a SQL database, using Apache Olingo.

### How do I get set up? ###

* Get [ODATA ABNF](http://docs.oasis-open.org/odata/odata/v4.0/errata02/os/complete/abnf/odata-abnf-construction-rules.txt)

	wget http://docs.oasis-open.org/odata/odata/v4.0/errata02/os/complete/abnf/odata-abnf-construction-rules.txt

    mvn install

### Who do I talk to? ###

* Repo owner or admin
* Other community or team contact

### TODO ###

- [ ] Rename `DatabaseMetaDataEdmProvider` to
      `ProtonMetaDataEdmProvider` or something.
- [ ] Rename all `Proton` parts of names to `ProtonChamber`.
- [ ] In `ProtonServlet` move setup code (like `Handler` registration)
      to the `init` method.
- [ ] Add proper and robust exception and error handling.
- [ ] Inject JNDI name of `DataSource` (i.e., "jdbc/ProtonDB") via
      `ServletConfig`.
- [ ] In `DatabaseMetaDataEdmProvider` audit and clean up data flow
      from JDBC `ResultSet` objects to Olingo objects.

