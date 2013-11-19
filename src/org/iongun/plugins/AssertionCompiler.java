package org.iongun.plugins;

import org.iongun.util.Translator;

public class AssertionCompiler implements Translator {
    public boolean acceptsInput (String input) {
	return ("" + input).matches("CREATE ASSERTION.*");}

    public String apply (String input) {
	String[] words = ("" + input)
	    .replaceAll("(", " ")
	    .replaceAll(")", " ")
	    .replaceAll(".", " ")
	    .replaceAll(";", " ")
	    .split(" ");
	return "";}}
