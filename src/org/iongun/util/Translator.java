package org.iongun.util;

import java.sql.Connection;

public interface Translator {
    public String apply (String input, Connection conn);}
