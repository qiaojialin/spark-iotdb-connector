package cn.edu.tsinghua.iotdb.query.analyzer;

import cn.edu.tsinghua.tsfile.qp.common.SQLConstant;
//import cn.edu.tsinghua.tsfile.common.utils.ITsRandomAccessFileReader;
import cn.edu.tsinghua.tsfile.qp.exception.*;
import cn.edu.tsinghua.tsfile.qp.common.FilterOperator;
import cn.edu.tsinghua.iotdb.query.analyzer.optimizer.logical.DNFFilterOptimizer;
import cn.edu.tsinghua.iotdb.query.analyzer.optimizer.logical.MergeSingleFilterOptimizer;
import cn.edu.tsinghua.iotdb.query.analyzer.optimizer.logical.RemoveNotOptimizer;
import cn.edu.tsinghua.iotdb.query.analyzer.optimizer.physical.PhysicalOptimizer;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;


public class Analyse {
    public List<TSFileQuery> analyse(FilterOperator filter, List<String> paths, String url, String deviceType, Long start, Long end)
            throws SQLException, ClassNotFoundException, LogicalOptimizeException, DNFOptimizeException, QueryProcessorException, QueryOperatorException, RemoveNotException, BasicOperatorException {

        List<TSFileQuery> allQuerys = new ArrayList<>();

        if(filter != null) {
            RemoveNotOptimizer removeNot = new RemoveNotOptimizer();
            try {
                filter = removeNot.optimize(filter);
            } catch (BasicOperatorException e) {
                e.printStackTrace();
            }

            DNFFilterOptimizer dnf = new DNFFilterOptimizer();
            filter = dnf.optimize(filter);
            MergeSingleFilterOptimizer merge = new MergeSingleFilterOptimizer();
            filter = merge.optimize(filter);

            List<FilterOperator> filterOperators = splitFilter(filter);

            PhysicalOptimizer physicalOptimizer = new PhysicalOptimizer();

            for (FilterOperator filterOperator : filterOperators) {
                SingleQuery singleQuery = constructSelectPlan(filterOperator);
                if (singleQuery != null) {
                    allQuerys.addAll(physicalOptimizer.optimize(singleQuery, paths, url, deviceType, start, end));
                }
            }
        } else {
            PhysicalOptimizer physicalOptimizer = new PhysicalOptimizer();
            allQuerys.addAll(physicalOptimizer.optimize(null, paths, url, deviceType, start, end));
        }


        return allQuerys;


    }

    private SingleQuery constructSelectPlan(FilterOperator filterOperator) throws QueryOperatorException {
        FilterOperator timeFilter = null;
        FilterOperator valueFilter = null;
        FilterOperator deltaObjectFilterOperator = null;
        List<FilterOperator> singleFilterList = null;
        if (filterOperator.isSingle()) {
            singleFilterList = new ArrayList<>();
            singleFilterList.add(filterOperator);
        } else if (filterOperator.getTokenIntType() == SQLConstant.KW_AND) {
            // now it has been dealt with merge optimizer, thus all nodes with same path have been
            // merged to one node
            singleFilterList = filterOperator.getChildren();
        }
        List<FilterOperator> valueList = new ArrayList<>();
        for (FilterOperator child : singleFilterList) {
            if (!child.isSingle()) {
                throw new QueryOperatorException(
                        "in format:[(a) and () and ()] or [] or [], a is not single! a:" + child);
            }
            switch (child.getSinglePath()) {
                case SQLConstant.RESERVED_TIME:
                    if (timeFilter != null) {
                        throw new QueryOperatorException(
                                "time filter has been specified more than once");
                    }
                    timeFilter = child;
                    break;
                case SQLConstant.RESERVED_DELTA_OBJECT:
                    if (deltaObjectFilterOperator != null) {
                        throw new QueryOperatorException(
                                "delta object filter has been specified more than once");
                    }
                    deltaObjectFilterOperator = child;
                    break;
                default:
                    valueList.add(child);
                    break;
            }
        }
        if (valueList.size() == 1) {
            valueFilter = valueList.get(0);
        } else if (valueList.size() > 1) {
            valueFilter = new FilterOperator(SQLConstant.KW_AND, false);
            valueFilter.childOperators = valueList;
        }

        return new SingleQuery(deltaObjectFilterOperator, timeFilter, valueFilter);
    }

    private List<FilterOperator> splitFilter(FilterOperator filterOperator) {
        List<FilterOperator> ret = new ArrayList<>();
        if (filterOperator.isSingle() || filterOperator.getTokenIntType() != SQLConstant.KW_OR) {
            ret.add(filterOperator);
            return ret;
        }
        // a list of partion linked with or
        return filterOperator.childOperators;
    }
}
