package query.analyzer.exception;

import com.corp.tsfile.qp.exception.QueryProcessorException;

/**
 * This exception is threw whiling meeting error in
 * {@linkplain com.corp.tsfile.qp.logical.operator.crud.BasicFunctionOperator BasicOperator}
 * 
 * @author kangrong
 *
 */
public class BasicOperatorException extends QueryProcessorException {

    private static final long serialVersionUID = -2163809754074237707L;

    public BasicOperatorException(String msg) {
        super(msg);
    }

}
