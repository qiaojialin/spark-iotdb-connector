package query.executor;

import com.corp.tsfile.read.file.TSRandomAccessFileReader;
import com.corp.tsfile.read.query.QueryConfig;
import com.corp.tsfile.read.query.QueryDataSet;
import com.corp.tsfile.read.query.QueryEngine;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Created by qiaojialin on 2016/12/1.
 */
public class Executor {
    public static List<QueryDataSet> query(TSRandomAccessFileReader in, List<QueryConfig> queryConfigs, Map<String, Object> parameters) throws IOException {
        QueryEngine queryEngine = new QueryEngine(in);

        List<QueryDataSet> dataSets = new ArrayList<>();

        for(QueryConfig queryConfig: queryConfigs) {

            List<String> cols = queryConfig.getSelectColumns();

            dataSets.add(queryEngine.query(queryConfig, parameters));
        }

        return dataSets;
    }
}
