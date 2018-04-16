package cn.edu.tsinghua.iotdb.query.analyzer;


import cn.edu.tsinghua.tsfile.qp.common.FilterOperator;

public class SingleQuery {

    private FilterOperator deltaObjectFilterOperator;
    private FilterOperator timeFilterOperator;
    private FilterOperator valueFilterOperator;

    public SingleQuery(FilterOperator deltaObjectFilterOperator,
                       FilterOperator timeFilter, FilterOperator valueFilter) {
        super();
        this.deltaObjectFilterOperator = deltaObjectFilterOperator;
        this.timeFilterOperator = timeFilter;
        this.valueFilterOperator = valueFilter;
    }

    public FilterOperator getDeltaObjectFilterOperator() {

        return deltaObjectFilterOperator;
    }

    public FilterOperator getTimeFilterOperator() {
        return timeFilterOperator;
    }

    public FilterOperator getValueFilterOperator() {
        return valueFilterOperator;
    }



}
