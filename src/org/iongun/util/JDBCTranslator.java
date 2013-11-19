package org.iongun.util;

import java.sql.Connection;

public interface JDBCTranslator {
    public String apply (String input, Connection conn);}

