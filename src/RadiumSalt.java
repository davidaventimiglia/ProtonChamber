import org.antlr.runtime.*;
import org.antlr.runtime.ANTLRInputStream;

public class RadiumSalt {
    public static void main (String[] args) throws Exception {
	ANTLRInputStream input = new ANTLRInputStream(System.in);
	TLexer lexer = new TLexer(input);
	CommonTokenStream tokens = new CommonTokenStream(lexer);
	TParser parser = new TParser(tokens);
	parser.r();}}

