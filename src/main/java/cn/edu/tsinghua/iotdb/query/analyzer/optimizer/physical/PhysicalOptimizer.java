package cn.edu.tsinghua.iotdb.query.analyzer.optimizer.physical;

import cn.edu.tsinghua.tsfile.qp.common.SQLConstant;
import java.sql.Connection;

import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
//import cn.edu.tsinghua.tsfile.jdbc.*;
import java.sql.SQLException;
//import cn.edu.tsinghua.tsfile.timeseries.read.management.SeriesSchema;
import cn.edu.tsinghua.iotdb.query.analyzer.TSFileQuery;
import cn.edu.tsinghua.tsfile.qp.common.BasicOperator;
import cn.edu.tsinghua.tsfile.qp.common.FilterOperator;
import cn.edu.tsinghua.iotdb.query.analyzer.SingleQuery;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;


public class PhysicalOptimizer {

    private static List<String> actualDeltaObjects = new ArrayList<>();

    public List<TSFileQuery> optimize(SingleQuery singleQuery, List<String> paths, String url, String deviceType, Long start, Long end) throws ClassNotFoundException, SQLException {

        List<String> validPaths = new ArrayList<>();
        for(String path: paths) {
            if(!path.equals(SQLConstant.RESERVED_DELTA_OBJECT) && !path.equals(SQLConstant.RESERVED_TIME)) {
                validPaths.add(path);
            }
        }

        Class.forName("cn.edu.tsinghua.iotdb.jdbc.TsfileDriver");
        Connection sqlConn = DriverManager.getConnection(url, "root", "root");
        DatabaseMetaData databaseMetaData = sqlConn.getMetaData();
        ResultSet resultSet = databaseMetaData.getColumns(null, null, deviceType, null);


        if(validPaths.size() == 0) {
            while(resultSet.next()) {
                validPaths.add(resultSet.getString("COLIMN_NAME"));
            }
        }

        List<String> allDeltaObjects = new ArrayList<>();
        ResultSet deltaObjects = databaseMetaData.getColumns(null, null, null, deviceType);
        while(deltaObjects.next()){
            allDeltaObjects.add(deltaObjects.getString("DELTA_OBJECT"));
        }
        sqlConn.close();

        FilterOperator timeFilter = null;
        FilterOperator valueFilter = null;
        if(singleQuery != null) {
            Set<String> selectDeltaObjects = mergeDeltaObject(singleQuery.getDeltaObjectFilterOperator());

            actualDeltaObjects.clear();
            //if select deltaObject, then match with measurement
            if(selectDeltaObjects != null && selectDeltaObjects.size() >= 1) {
                actualDeltaObjects.addAll(selectDeltaObjects);
            } else {
                actualDeltaObjects.addAll(allDeltaObjects); //@@@@@@@add start and end parameters
            }

            timeFilter = singleQuery.getTimeFilterOperator();
            valueFilter = singleQuery.getValueFilterOperator();
        } else {
            actualDeltaObjects.addAll(allDeltaObjects);
        }


        List<TSFileQuery> tsFileQueries = new ArrayList<>();
        for(String deltaObject: actualDeltaObjects) {
            List<String> newPaths = new ArrayList<>();
            for(String path: validPaths) {
                String newPath = deltaObject + "." + path;
                newPaths.add(newPath);
            }
            if(valueFilter == null) {
                tsFileQueries.add(new TSFileQuery(newPaths, timeFilter, null));
            } else {
                FilterOperator newValueFilter = valueFilter.clone();
                newValueFilter.addHeadDeltaObjectPath(deltaObject);
                tsFileQueries.add(new TSFileQuery(newPaths, timeFilter, newValueFilter));
            }
        }
        return tsFileQueries;
    }


    public Set<String> mergeDeltaObject(FilterOperator deltaFilterOperator) {
        if (deltaFilterOperator == null) {
            System.out.println("deltaFilterOperator is null");
            return null;
        }
        if (deltaFilterOperator.isLeaf()) {
            Set<String> r = new HashSet<String>();
            r.add(((BasicOperator)deltaFilterOperator).getSeriesValue());
            return r;
        }
        List<FilterOperator> children = deltaFilterOperator.getChildren();
        if (children.isEmpty()) {
            return new HashSet<String>();
        }
        Set<String> ret = mergeDeltaObject(children.get(0));
        for (int i = 1; i < children.size(); i++) {
            Set<String> temp = mergeDeltaObject(children.get(i));
            switch (deltaFilterOperator.getTokenIntType()) {
                case SQLConstant.KW_AND:
                    ret.retainAll(temp);
                    break;
                case SQLConstant.KW_OR:
                    ret.addAll(temp);
                    break;
                default:
                    throw new UnsupportedOperationException("given error token type:"+deltaFilterOperator.getTokenIntType());
            }
        }
        return ret;
    }
}
