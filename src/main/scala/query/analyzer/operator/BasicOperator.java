package query.analyzer.operator;

import com.corp.tsfile.qp.constant.SQLConstant;
import query.analyzer.exception.BasicOperatorException;

/**
 * basic operator include < > >= <= !=.
 * 
 * @author kangrong
 *
 */

public class BasicOperator extends FilterOperator {

    protected String seriesPath;
    protected String seriesValue;

    public String getSeriesPath() {
        return seriesPath;
    }

    public String getSeriesValue() {
        return seriesValue;
    }

    public BasicOperator(int tokenIntType, String path, String value)
            throws BasicOperatorException {
        super(tokenIntType);
        this.seriesPath = this.singlePath = path;
        this.seriesValue = value;
        this.isLeaf = true;
        this.isSingle = true;
    }

    public void setReversedTokenIntType() throws BasicOperatorException, com.corp.tsfile.qp.exception.logical.operator.BasicOperatorException {
        int intType = SQLConstant.reverseWords.get(tokenIntType);
        setTokenIntType(intType);
    }

    @Override
    public String getSinglePath() {
        return singlePath;
    }


    @Override
    public BasicOperator clone() {
        BasicOperator ret;
        try {
            ret = new BasicOperator(this.tokenIntType, seriesPath, seriesValue);
        } catch (BasicOperatorException e) {
            return null;
        }
        ret.tokenSymbol=tokenSymbol;
        ret.isLeaf = isLeaf;
        ret.isSingle = isSingle;
        return ret;
    }
    
    @Override
    public String toString() {
        return "[" + seriesPath + tokenSymbol + seriesValue + "]";
    }
}
