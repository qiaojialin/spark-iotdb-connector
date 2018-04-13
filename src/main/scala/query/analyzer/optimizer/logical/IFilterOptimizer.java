package query.analyzer.optimizer.logical;

import com.corp.tsfile.qp.exception.logical.operator.BasicOperatorException;
import query.analyzer.exception.DNFOptimizeException;
import query.analyzer.exception.LogicalOptimizeException;
import query.analyzer.exception.QueryProcessorException;
import query.analyzer.exception.RemoveNotException;
import query.analyzer.operator.FilterOperator;

/**
 * provide a filter operator, optimize it.
 * 
 * @author kangrong
 *
 */
public interface IFilterOptimizer {
    public FilterOperator optimize(FilterOperator filter) throws QueryProcessorException, DNFOptimizeException, LogicalOptimizeException, RemoveNotException, BasicOperatorException;
}
