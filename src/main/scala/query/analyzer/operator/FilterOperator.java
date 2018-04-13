package query.analyzer.operator;

import com.corp.tsfile.utils.StringContainer;

import java.util.ArrayList;
import java.util.List;


public class FilterOperator extends Operator implements Comparable<FilterOperator> {


    public List<FilterOperator> childOperators;
    // leaf filter operator means it doesn't have left and right child filterOperator. Leaf filter
    // should set FunctionOperator.
    protected boolean isLeaf = false;
    // isSingle being true means all recursive children of this filter belong to one series path.
    protected boolean isSingle = false;
    // if isSingle = false, singlePath must be null
    protected String singlePath = null;

    public FilterOperator(int tokenType) {
        super(tokenType);
        childOperators = new ArrayList<>();
    }
    
    public void setTokenIntType(int intType)  {
        super.tokenIntType = intType;
    }

    public FilterOperator(int tokenType, boolean isSingle) {
        this(tokenType);
        this.isSingle = isSingle;
    }

    public void addHeadDeltaObjectPath(String deltaObject) {
        for (FilterOperator child : childOperators) {
            child.addHeadDeltaObjectPath(deltaObject);
        }
        if(isSingle) {
            this.singlePath = deltaObject + "." + this.singlePath;
        }
    }

    public List<FilterOperator> getChildren() {
        return childOperators;
    }

    public void setChildrenList(List<FilterOperator> children) {
        this.childOperators = children;
    }

    public void setIsSingle(boolean b) {
        this.isSingle = b;
    }

    /**
     * if this node's singlePath is set, this.isLeaf will be set true in same time
     * 
     * @param p
     */
    public void setSinglePath(String p) {
        this.singlePath = p;
    }

    public String getSinglePath() {
        return singlePath;
    }

    public boolean addChildOPerator(FilterOperator op) {
        childOperators.add((FilterOperator) op);
        return true;
    }


    /**
     * if this is null, ordered to later
     */
    @Override
    public int compareTo(FilterOperator fil) {
        if (singlePath == null && fil.singlePath == null) {
            return 0;
        }
        if (singlePath == null) {
            return 1;
        }
        if (fil.singlePath == null) {
            return -1;
        }
        return fil.singlePath.compareTo(singlePath);
    }

    public boolean isLeaf() {
        return isLeaf;
    }
    
    public boolean isSingle() {
        return isSingle;
    }

    @Override
    public String toString() {
        StringContainer sc = new StringContainer();
        sc.addTail("[", this.tokenSymbol);
        if (isSingle) {
            sc.addTail("[single:", getSinglePath().toString(), "]");
        }
        sc.addTail(" ");
        for (FilterOperator filter : childOperators) {
            sc.addTail(filter.toString());
        }
        sc.addTail("]");
        return sc.toString();
    }
    
    @Override
    public FilterOperator clone() {
        FilterOperator ret = new FilterOperator(this.tokenIntType);
        ret.tokenSymbol=tokenSymbol;
        ret.isLeaf = isLeaf;
        ret.isSingle = isSingle;
        if(singlePath != null)
            ret.singlePath = singlePath;
        for (FilterOperator filterOperator : this.childOperators) {
            ret.addChildOPerator(filterOperator.clone());
        }
        return ret;
    }
}
