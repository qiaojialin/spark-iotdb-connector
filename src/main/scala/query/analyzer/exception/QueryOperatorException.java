package query.analyzer.exception;

import com.corp.tsfile.qp.exception.QueryProcessorException;

/**
 * This exception is threw whiling meeting error in
 * {@linkplain com.corp.tsfile.qp.logical.operator.crud.QueryOperator QueryOperator}
 * 
 * @author kangrong
 *
 */
public class QueryOperatorException extends QueryProcessorException {

    private static final long serialVersionUID = 4466703029858082216L;

    public QueryOperatorException(String msg) {
        super(msg);
    }

}
