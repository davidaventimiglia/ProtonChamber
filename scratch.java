import apg.*;
import apg.Parser.*;
import org.protonchamber.*;
Parser parser = new Parser(Test.getInstance());
parser.setStartRule(Test.RuleNames.DUMMYSTARTRULE.ruleID());
parser.setInputString("People('russellwhyte')");
Ast ast = parser.enableAst(true);
for (Test.RuleNames r : Test.RuleNames.values()) ast.enableRuleNode(r.ruleID(), true);
Result result = parser.parse();


// try (ResultSet r = m.getPrimaryKeys(null, null, null)) {
//     while (r.next())
// 	System.out.println(String.format("%s,%s,%s", r.getString("TABLE_SCHEM"), r.getString("TABLE_NAME"), r.getString("COLUMN_NAME")));} catch (Throwable t) {}
