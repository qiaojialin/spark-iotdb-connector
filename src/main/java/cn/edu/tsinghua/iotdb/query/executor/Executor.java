package cn.edu.tsinghua.iotdb.query.executor;

import cn.edu.tsinghua.tsfile.common.utils.ITsRandomAccessFileReader;
import cn.edu.tsinghua.tsfile.timeseries.read.query.QueryConfig;
import cn.edu.tsinghua.tsfile.timeseries.read.query.QueryDataSet;
import cn.edu.tsinghua.tsfile.timeseries.read.query.QueryEngine;
import scala.tools.cmd.gen.AnyVals;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Created by qiaojialin on 2016/12/1.
 */
public class Executor {
    public static List<QueryDataSet> query(ITsRandomAccessFileReader in, List<QueryConfig> queryConfigs, Map<String, Long> parameters) throws IOException {
        QueryEngine queryEngine = new QueryEngine(in);

        List<QueryDataSet> dataSets = new ArrayList<>();

        for(QueryConfig queryConfig: queryConfigs) {

            List<String> cols = queryConfig.getSelectColumns();

            dataSets.add(queryEngine.query(queryConfig, parameters));
        }

        return dataSets;
    }
}
