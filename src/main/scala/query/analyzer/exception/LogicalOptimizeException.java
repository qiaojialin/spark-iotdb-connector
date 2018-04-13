package query.analyzer.exception;

import com.corp.tsfile.qp.exception.QueryProcessorException;

public class LogicalOptimizeException extends QueryProcessorException {

    private static final long serialVersionUID = -7098092782689670064L;

    public LogicalOptimizeException(String msg) {
        super(msg);
    }

}
