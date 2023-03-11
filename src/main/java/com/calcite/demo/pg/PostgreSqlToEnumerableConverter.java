package com.calcite.demo.pg;

import org.apache.calcite.adapter.enumerable.*;
import org.apache.calcite.linq4j.tree.BlockBuilder;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.linq4j.tree.MethodCallExpression;
import org.apache.calcite.plan.*;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterImpl;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.runtime.Hook;
import org.apache.calcite.util.BuiltInMethod;
import org.apache.calcite.util.Pair;

import java.util.AbstractList;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * 描述：PostgreSqlToEnumerableConverter是逻辑执行计划的根节点
 *
 * 1.将SqlQuery也就是SqlNode转换成RelNode,为接下来的优化做准备；
 * 2.PostgreSqlToEnumerableConverter是逻辑执行计划的根节点，该算子以下都是我们自己定义的可以下推的RelNode.其上都是无法下推的RelNode.
 * 3.生成物理执行计划的时候，会调用PostgreSqlToEnumerableConverter的implement方法，implement继续调用visitChild方法访问其子节点。
 * 4.在递归返回其自节点的时候，每个节点信息都保存在Implementor之中。
 * 5.从Implementor拿到所有节点信息后，利用Ling4j会调用过程生成一个一个的表达式。
 * 6.生成这一个个表达式之后，会调用PostgreSqlQueryable#query()方法，返回封装好的迭代器，该表达式会转化成字符串交给Janino,在内存中
 * 实时编译并执行
 *
 */
public class PostgreSqlToEnumerableConverter extends ConverterImpl implements EnumerableRel {


    public PostgreSqlToEnumerableConverter(RelOptCluster cluster, RelTraitSet traitSet, RelNode child) {
        super(cluster, ConventionTraitDef.INSTANCE,traitSet,child);
    }


    /**
     * 构建表达式树
     *
     * @param implementor Implementor
     * @param pref Preferred representation for rows in result expression
     * @return
     */
    @Override
    public Result implement(EnumerableRelImplementor implementor, Prefer pref) {
        final BlockBuilder list = new BlockBuilder();
        IPostgreSqlRel.Implementor pgImplementor = new IPostgreSqlRel.Implementor();
        /**
         * visitChild方法访问规则的子节点，子节点又会访问下面的子节点，如此递归下去
         * PostgreSqlToEnumerableConverter -> 获取Implementor报存的所有信息
         *      ｜
         *  PostgreSqlLimit                -> 获取Implementor保存的Limit信息
         *      ｜
         *  PostgreSqlFilter               -> 获取Implementor保存的Filter信息
         *      ｜
         *  PostgreSqlTableScan            -> 获取Implementor保存的表信息
         *
         */
        pgImplementor.visitChild(0,getInput());
        RelDataType rowType = getRowType();
        PhysType physType = PhysTypeImpl.of(implementor.getTypeFactory(), rowType, pref.prefer(JavaRowFormat.ARRAY));
        final Expression fields = list.append("FIELDS",
                constantArrayList(Pair.zip(PostgreSqlRules.postgreSqlFieldNames(rowType),
                        new AbstractList<Class<?>>() {
                            @Override
                            public Class<?> get(int index) {
                                return physType.fieldClass(index);
                            }

                            @Override
                            public int size() {
                                return rowType.getFieldCount();
                            }
                        }), Pair.class));

        ArrayList<Map.Entry<String, String>> selectList = new ArrayList<>();
        Pair.zip(pgImplementor.selectFields.keySet(), pgImplementor.selectFields.values()).forEach(selectList::add);
        final Expression table = list.append("TABLE", pgImplementor.table.getExpression(PostgreSqlQueryable.class));
        final Expression selectFields = list.append("SELECT_FIELDS", constantArrayList(selectList, Pair.class));
        final Expression predicates = list.append("PREDICATES", constantArrayList(pgImplementor.whereClause, String.class));
        final Expression offset = list.append("OFFSET", Expressions.constant(pgImplementor.offset));
        final Expression fetch = list.append("FETCH", Expressions.constant(pgImplementor.fetch));
        final Expression order = list.append("ORDER", constantArrayList(pgImplementor.order, String.class));
        final Expression aggregate = list.append("AGGREGATE", constantArrayList(pgImplementor.agg, String.class));
        final Expression group = list.append("GROUP", constantArrayList(pgImplementor.group, String.class));
        final Expression enumerable = list.append("ENUMERABLE", Expressions.call(table, PostgreSqlMethod.PGMethod_QUERYABLE_QUERY.method,
                fields, selectFields, offset, fetch, aggregate, group, predicates, order));

        list.add(Expressions.return_(null,enumerable));
        Hook.QUERY_PLAN.run(predicates);
        return implementor.result(physType,list.toBlock());
    }

    @Override
    public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
        return super.copy(traitSet, inputs);
    }

    @Override
    public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
        return super.computeSelfCost(planner, mq);
    }

    /**
     * 将list转换为表达式
     *
     * @param values
     * @param clazz
     * @return
     * @param <T>
     */
    private <T> MethodCallExpression constantArrayList(List<T> values, Class<?> clazz) {
        return Expressions.call(BuiltInMethod.ARRAYS_AS_LIST.method,
                Expressions.newArrayInit(clazz,constantList(values)));
    }

    /**
     * 将List转为常量
     * @param values
     * @return
     * @param <T>
     */
    private <T> List<? extends Expression> constantList(List<T> values) {
        return values.stream().map(Expressions::constant)
                .collect(Collectors.toList());
    }


}
