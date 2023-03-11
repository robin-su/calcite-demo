package com.calcite.demo.pg;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.plan.*;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.type.RelDataType;

import java.util.List;

public class PostgreSqlTableScan extends TableScan implements IPostgreSqlRel {
    private PostgreSqlTable postgreSqlTable;
    private RelDataType projectRowType;


    public PostgreSqlTableScan(PostgreSqlTable postgreSqlTable,
                               RelDataType projectRowType,
                               RelOptCluster cluster,
                               RelTraitSet traitSet,
                               RelOptTable table) {
        super(cluster,traitSet,ImmutableList.of(),table);
        this.postgreSqlTable = postgreSqlTable;
        this.projectRowType = projectRowType;
    }

    /**
     * 当访问到叶子节点的时候，会记录当前需要查询的表的详细信息
     */
    @Override
    public void implement(Implementor implementor) {
        implementor.postgreSqlTable = postgreSqlTable;
        implementor.table = table;
    }

    @Override
    public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
        assert inputs.isEmpty();
        return this;
    }

    /**
     *  注册规则
     *
     * @param planner Planner to be used to register additional relational
     *                expressions
     */
    @Override
    public void register(RelOptPlanner planner) {
        planner.addRule(PostgreSqlToEnumerableConverterRule.INSTANCE);
        for (RelOptRule rule : PostgreSqlRules.RULES) {
            planner.addRule(rule);
        }
    }

    @Override
    public RelDataType deriveRowType() {
        return projectRowType == null
                ? super.deriveRowType()
                :projectRowType;
    }
}
