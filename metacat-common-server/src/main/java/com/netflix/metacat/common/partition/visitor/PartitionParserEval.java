package com.netflix.metacat.common.partition.visitor;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.netflix.metacat.common.partition.parser.ASTAND;
import com.netflix.metacat.common.partition.parser.ASTEQ;
import com.netflix.metacat.common.partition.parser.ASTEVAL;
import com.netflix.metacat.common.partition.parser.ASTFILTER;
import com.netflix.metacat.common.partition.parser.ASTGT;
import com.netflix.metacat.common.partition.parser.ASTGTE;
import com.netflix.metacat.common.partition.parser.ASTLIKE;
import com.netflix.metacat.common.partition.parser.ASTLT;
import com.netflix.metacat.common.partition.parser.ASTLTE;
import com.netflix.metacat.common.partition.parser.ASTMATCHES;
import com.netflix.metacat.common.partition.parser.ASTNEQ;
import com.netflix.metacat.common.partition.parser.ASTNEVAL;
import com.netflix.metacat.common.partition.parser.ASTNUM;
import com.netflix.metacat.common.partition.parser.ASTOR;
import com.netflix.metacat.common.partition.parser.ASTSTRING;
import com.netflix.metacat.common.partition.parser.ASTVAR;
import com.netflix.metacat.common.partition.parser.PartitionParserVisitor;
import com.netflix.metacat.common.partition.parser.SimpleNode;
import com.netflix.metacat.common.partition.parser.Variable;

import java.math.BigDecimal;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class PartitionParserEval implements PartitionParserVisitor {
    public static final Pattern likePattern = Pattern.compile("(\\[%\\]|\\[_\\]|\\[\\[\\]|%|_)");
    public static final Map<String, String> likeToRegexReplacements = new ImmutableMap.Builder<String, String>()
            .put("[%]", "%")
            .put("[_]", "_")
            .put("[[]", "[")
            .put("%", ".*")
            .put("_", ".").build();
    public enum Compare {
        EQ("="), GT(">"), GTE(">="), LT("<"), LTE("<="), NEQ("!="), MATCHES("MATCHES"), LIKE("LIKE");
        String expression;
        Compare(String expression) {
            this.expression = expression;
        }
        public String getExpression(){
            return expression;
        }
    }

	private Map<String, String> context;

	public PartitionParserEval() {
        this(Maps.newHashMap());
	}
	public PartitionParserEval(Map<String, String> context) {
		this.context = context;
	}

	public Boolean eval(SimpleNode node, Object data) {
		Object value1 = node.jjtGetChild(0).jjtAccept(this, data);
		Compare comparison = (Compare) node.jjtGetChild(1).jjtAccept(this, data);
		Object value2 = node.jjtGetChild(2).jjtAccept(this, data);
		if (value2 instanceof String) {
			return compare(comparison, value1.toString(), value2.toString());
		}
		if (value2 instanceof BigDecimal) {
			if (value1 instanceof String) {
				value1 = new BigDecimal(value1.toString());
			}
			return compare(comparison, (BigDecimal) value1, (BigDecimal) value2);
		}
		throw new RuntimeException("error processing partition filter");
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	public Boolean compare(Compare comparison, Comparable value1, Comparable value2) {
		if (value1 == null) {
			switch (comparison) {
			case EQ:
            case LIKE:
            case MATCHES:
				return value2 == null;
			case NEQ:
				return value2 != null;
			default:
				return false;
			}
		}
        if( comparison.equals(Compare.LIKE) || comparison.equals(Compare.MATCHES)){
            if( value2 != null){
                String value = value2.toString();
                if(comparison.equals(Compare.LIKE)){
                    value = sqlLiketoRegexExpression(value);
                }
                return value1.toString().matches(value);
            }
        } else {
            int compare = value1.compareTo(value2);
            switch (comparison) {
            case GT:
                return compare > 0;
            case GTE:
                return compare >= 0;
            case LT:
                return compare < 0;
            case LTE:
                return compare <= 0;
            case EQ:
                return compare == 0;
            case NEQ:
                return compare != 0;
            }
        }
		return false;
	}

    //TODO: Need to escape regex meta characters
    protected String sqlLiketoRegexExpression(String likeExpression) {
        Matcher m = likePattern.matcher(likeExpression);

        StringBuffer builder = new StringBuffer();
        while(m.find()){
            m.appendReplacement(builder, likeToRegexReplacements.get(m.group()));
        }
        m.appendTail(builder);
        return builder.toString();
    }

    @Override
	public Object visit(ASTAND node, Object data) {
		Boolean v1 = (Boolean) node.jjtGetChild(0).jjtAccept(this, data);
		return v1 && (Boolean) node.jjtGetChild(1).jjtAccept(this, data);
	}

	@Override
	public Object visit(ASTEQ node, Object data) {
		return Compare.EQ;
	}

	@Override
	public Object visit(ASTEVAL node, Object data) {
		return eval(node, data);
	}

	@Override
	public Object visit(ASTFILTER node, Object data) {
		return node.jjtGetChild(0).jjtAccept(this, data);
	}

	@Override
	public Object visit(ASTGT node, Object data) {
		return Compare.GT;
	}

	@Override
	public Object visit(ASTGTE node, Object data) {
		return Compare.GTE;
	}

	@Override
	public Object visit(ASTLT node, Object data) {
		return Compare.LT;
	}

	@Override
	public Object visit(ASTLTE node, Object data) {
		return Compare.LTE;
	}

	@Override
	public Object visit(ASTNEQ node, Object data) {
		return Compare.NEQ;
	}

    @Override
    public Object visit(ASTMATCHES node, Object data) {
        return Compare.MATCHES;
    }

    @Override
    public Object visit(ASTLIKE node, Object data) {
        return Compare.LIKE;
    }

    @Override
	public Object visit(ASTNEVAL node, Object data) {
		return !(Boolean) node.jjtGetChild(0).jjtAccept(this, data);
	}

	@Override
	public Object visit(ASTNUM node, Object data) {
		return node.jjtGetValue();
	}

	@Override
	public Object visit(ASTOR node, Object data) {
		Boolean v1 = (Boolean) node.jjtGetChild(0).jjtAccept(this, data);
		return v1 || (Boolean) node.jjtGetChild(1).jjtAccept(this, data);
	}

	@Override
	public Object visit(ASTSTRING node, Object data) {
		return node.jjtGetValue();
	}

	@Override
	public Object visit(ASTVAR node, Object data) {
	    if (!context.containsKey(((Variable)node.jjtGetValue()).getName())) {
	        throw new RuntimeException("Missing variable: " + ((Variable)node.jjtGetValue()).getName());
	    }
		return context.get(((Variable)node.jjtGetValue()).getName());
	}

	@Override
	public Object visit(SimpleNode node, Object data) {
		return null;
	}

}
