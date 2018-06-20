package org.protonchamber;

import apg.*;
import apg.Parser.*;
import apg.Parser.Result;
import java.io.*;
import java.util.*;

public class ProtonChamber {
    public static void main (String[] args) throws Exception {
	Scanner input = new Scanner(System.in);
	while (input.hasNext()) {
	    process(input.nextLine(), System.out);}}

    public static void process (String s, PrintStream out) throws Exception {
	Parser parser = new Parser(Test.getInstance());
	parser.setStartRule(Test.RuleNames.DUMMYSTARTRULE.ruleID());
	parser.setInputString(s);
	Ast ast = parser.enableAst(true);
	for (Test.RuleNames r : Test.RuleNames.values()) ast.enableRuleNode(r.ruleID(), true);
	ast.enableRuleNode(Test.RuleNames.DUMMYSTARTRULE.ruleID(), true);
	Result result = parser.parse();
	ast.display(out, true);}}
