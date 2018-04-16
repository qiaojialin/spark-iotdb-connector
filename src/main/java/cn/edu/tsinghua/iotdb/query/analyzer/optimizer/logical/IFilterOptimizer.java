package cn.edu.tsinghua.iotdb.query.analyzer.optimizer.logical;

import cn.edu.tsinghua.tsfile.qp.exception.BasicOperatorException;
import cn.edu.tsinghua.tsfile.qp.exception.DNFOptimizeException;
import cn.edu.tsinghua.tsfile.qp.exception.LogicalOptimizeException;
import cn.edu.tsinghua.tsfile.qp.exception.QueryProcessorException;
import cn.edu.tsinghua.tsfile.qp.exception.RemoveNotException;
import cn.edu.tsinghua.tsfile.qp.common.FilterOperator;

/**
 * provide a filter operator, optimize it.
 * 
 * @author kangrong
 *
 */
public interface IFilterOptimizer {
    public FilterOperator optimize(FilterOperator filter) throws QueryProcessorException, DNFOptimizeException, LogicalOptimizeException, RemoveNotException, BasicOperatorException;
}
