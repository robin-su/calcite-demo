package com.calcite.demo.pg;


import org.apache.calcite.adapter.java.AbstractQueryableTable;
import org.apache.calcite.linq4j.QueryProvider;
import org.apache.calcite.linq4j.Queryable;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.TranslatableTable;
import org.apache.calcite.sql.type.SqlTypeName;

import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.List;

/**
 * ScannabelTable: 该接口很简单，在查询数据的时候不会产生任何中间表达式，而是把全部数据放到内存中计算；Redis数据源便是实现该接口；
 * FilterableTable: 根据拿到的过滤条件对查询的数据做进一步的过滤，也就是拿到过滤条件后在数据源就对数据进行过滤，返回迭代器便是过滤后的结果；
 * TranslatableTable: 可以将聚合、排序、投影等操作下推到数据源来完成，例如Postgresql就适合这种；
 */
public class PostgreSqlTable extends AbstractQueryableTable implements TranslatableTable {

    private List<CdmColumn> columns;
    private PostgreSqlInfo info;

    public PostgreSqlTable(List<CdmColumn> columns,PostgreSqlInfo info) {
        super(Type.class);
        this.columns = columns;
        this.info = info;
    }

    @Override
    public <T> Queryable<T> asQueryable(QueryProvider queryProvider, SchemaPlus schema, String tableName) {
        return new PostgreSqlQueryable<>(info, queryProvider,schema,this,tableName);
    }

    /**
     * 将TableScan转换成我们自定义的Scan算子，之后在此基础上创建自己注册的规则和RelNode,如Filter算子，Sort算子等
     *
     * @param context
     * @param relOptTable
     * @return
     */
    @Override
    public RelNode toRel(RelOptTable.ToRelContext context, RelOptTable relOptTable) {
        RelOptCluster cluster = context.getCluster();
        return new PostgreSqlTableScan(this, null,cluster,cluster.traitSetOf(IPostgreSqlRel.CONVENTION),relOptTable);
    }

    @Override
    public RelDataType getRowType(RelDataTypeFactory typeFactory) {
        ArrayList<String> names = new ArrayList<>();
        ArrayList<RelDataType> types = new ArrayList<>();
        columns.forEach(c -> {
            names.add(c.getName());
            types.add(typeFactory.createSqlType(SqlTypeName.get(c.getType().toUpperCase())));
        });
        return typeFactory.createStructType(types, names);
    }
}
