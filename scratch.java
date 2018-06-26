import apg.*;
import apg.Parser.*;
import org.protonchamber.*;
Parser parser = new Parser(Test.getInstance());
parser.setStartRule(Test.RuleNames.DUMMYSTARTRULE.ruleID());
parser.setInputString("People('russellwhyte')");
Ast ast = parser.enableAst(true);
for (Test.RuleNames r : Test.RuleNames.values()) ast.enableRuleNode(r.ruleID(), true);
Result result = parser.parse();
