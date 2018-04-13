package query.analyzer.operator;

import com.corp.tsfile.qp.constant.SQLConstant;

/**
 * This class is a superclass of all operator. 
 * @author kangrong
 *
 */
public abstract class Operator {
    protected int tokenIntType;
    protected String tokenSymbol;

    public Operator(int tokenIntType) {
        this.tokenIntType = tokenIntType;
        this.tokenSymbol = SQLConstant.tokenSymbol.get(tokenIntType);
    }

    public int getTokenIntType() {
        return tokenIntType;
    }
    
    public String getTokenSymbol() {
        return tokenSymbol;
    }

    @Override
    public String toString() {
        return tokenSymbol;
    }
}
