package query.analyzer.exception;


import com.corp.tsfile.qp.exception.logical.optimize.LogicalOptimizeException;

public class DNFOptimizeException extends LogicalOptimizeException {

    private static final long serialVersionUID = 807384397361662482L;

    public DNFOptimizeException(String msg) {
        super(msg);
    }

}
