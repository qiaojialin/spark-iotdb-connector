package query.analyzer.exception;


import com.corp.tsfile.qp.exception.logical.optimize.LogicalOptimizeException;

public class RemoveNotException extends LogicalOptimizeException {


    /**
     * 
     */
    private static final long serialVersionUID = -772591029262375715L;

    public RemoveNotException(String msg) {
        super(msg);
    }

}
