package query.analyzer;

import query.analyzer.operator.FilterOperator;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by qiaojialin on 2016/11/30.
 */
public class TSFileQuery {
    private List<String> paths = new ArrayList<>();
    private FilterOperator timeFilterOperator;
    private FilterOperator valueFilterOperator;

    public TSFileQuery(List<String> paths, FilterOperator timeFilter, FilterOperator valueFilter) {
        this.paths = paths;
        this.timeFilterOperator = timeFilter;
        this.valueFilterOperator = valueFilter;
    }

    public List<String> getPaths() {
        return paths;
    }

    public FilterOperator getTimeFilterOperator() {
        return timeFilterOperator;
    }

    public FilterOperator getValueFilterOperator() {
        return valueFilterOperator;
    }

    public String toString(){
        StringBuffer str;
        return "";
    }
}
