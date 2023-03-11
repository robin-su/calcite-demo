package com.calcite.demo.pg;

import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public interface IPostgreSqlRel extends RelNode {

    void implement(IPostgreSqlRel.Implementor implementor);

    Convention CONVENTION  = new Convention.Impl("PG",IPostgreSqlRel.class);

    /**
     * 记录每个下推算子的内容，每个列表的泛型都是String类型，为了后续拼接SQL的时候方便
     */
    class Implementor {
        /**
         * select 字段
         */
        final Map<String,String> selectFields = new LinkedHashMap<>();
        /**
         * where 字段
         */
        final List<String> whereClause = new ArrayList<>();
        /**
         * 偏移量
         */
        int offset = 0;
        /**
         * limit字段
         */
        int fetch = -1;
        /**
         * order字段
         */
        final List<String> order = new ArrayList<>();
        /**
         * 聚合字段
         */
        final List<String> agg = new ArrayList<>();
        /**
         * group by字段
         */
        final List<String> group = new ArrayList<>();
        /**
         * 表信息
         */
        RelOptTable table;
        /**
         * pg 表
         */
        PostgreSqlTable postgreSqlTable;

        /**
         * 添加字段
         *
         * @param fields     字段映射
         * @param predicates 过滤条件
         */
        public void add(Map<String,String> fields,List<String> predicates) {
            if(fields != null) {
                selectFields.putAll(fields);
            }
            if(predicates != null) {
                whereClause.addAll(predicates);
            }
        }

        /**
         * 添加聚合运算符
         *
         * @param aggOp 较聚合运算符
         */
        public void add(String aggOp) {
            agg.add(aggOp);
        }

        /**
         * 添加分组字段
         *
         * @param groupOp 分组字段
         */
        public void addGroup(String groupOp) {
            group.add(groupOp);
        }

        /**
         * 添加排序列表
         *
         * @param newOrder 排序字段
         */
        public void addOrder(List<String> newOrder) {
            order.addAll(newOrder);
        }

        public void visitChild(int ordinal, RelNode input) {
            assert ordinal == 0;
            assert input instanceof IPostgreSqlRel;
            ((IPostgreSqlRel)input).implement(this);
        }
    }

}
