package query.analyzer.exception;


import com.corp.tsfile.qp.exception.logical.optimize.LogicalOptimizeException;

public class MergeFilterException extends LogicalOptimizeException {

    private static final long serialVersionUID = 8581594261924961899L;

    public MergeFilterException(String msg) {
        super(msg);
    }

}
