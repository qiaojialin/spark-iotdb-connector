package query.analyzer.optimizer.logical;

import com.corp.tsfile.qp.constant.SQLConstant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import query.analyzer.exception.BasicOperatorException;
import query.analyzer.exception.LogicalOptimizeException;
import query.analyzer.exception.RemoveNotException;
import query.analyzer.operator.BasicOperator;
import query.analyzer.operator.FilterOperator;

import java.util.List;

import static com.corp.tsfile.qp.constant.SQLConstant.*;

public class RemoveNotOptimizer implements IFilterOptimizer {
    private static final Logger LOG = LoggerFactory.getLogger(RemoveNotOptimizer.class);


    /**
     * get DNF(disjunctive normal form) for this filter operator tree. Before getDNF, this op tree
     * must be binary, in another word, each non-leaf node has exactly two children.
     * 
     * @return
     * @throws LogicalOptimizeException
     */
    @Override
    public FilterOperator optimize(FilterOperator filter) throws LogicalOptimizeException, RemoveNotException, com.corp.tsfile.qp.exception.logical.operator.BasicOperatorException {
        return removeNot(filter);
    }

    private FilterOperator removeNot(FilterOperator filter) throws RemoveNotException, com.corp.tsfile.qp.exception.logical.operator.BasicOperatorException {
        if (filter.isLeaf())
            return filter;
        int tokenInt = filter.getTokenIntType();
        switch (tokenInt) {
            case KW_AND:
            case KW_OR:
                // replace children in-place for efficiency
                List<FilterOperator> children = filter.getChildren();
                children.set(0, removeNot(children.get(0)));
                children.set(1, removeNot(children.get(1)));
                return filter;
            case KW_NOT:
                return reverseFilter(filter.getChildren().get(0));
            default:
                throw new RemoveNotException("Unknown token in removeNot: " + tokenInt + ","
                        + SQLConstant.tokenNames.get(tokenInt));
        }
    }


    private FilterOperator reverseFilter(FilterOperator filter) throws RemoveNotException, com.corp.tsfile.qp.exception.logical.operator.BasicOperatorException {
        int tokenInt = filter.getTokenIntType();
        if (filter.isLeaf()) {
            try {
                ((BasicOperator) filter).setReversedTokenIntType();
            } catch (BasicOperatorException e) {
                throw new RemoveNotException(
                        "convert BasicFuntion to reserved meet failed: previous token:" + tokenInt
                                + "tokenSymbol:" + SQLConstant.tokenNames.get(tokenInt));
            }
            return filter;
        }
        switch (tokenInt) {
            case KW_AND:
            case KW_OR:
                List<FilterOperator> children = filter.getChildren();
                children.set(0, reverseFilter(children.get(0)));
                children.set(1, reverseFilter(children.get(1)));
                filter.setTokenIntType(SQLConstant.reverseWords.get(tokenInt));
                return filter;
            case KW_NOT:
                return removeNot(filter.getChildren().get(0));
            default:
                throw new RemoveNotException("Unknown token in reverseFilter: " + tokenInt + ","
                        + SQLConstant.tokenNames.get(tokenInt));
        }
    }

}
