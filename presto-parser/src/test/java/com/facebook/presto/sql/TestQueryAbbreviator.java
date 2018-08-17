package com.facebook.presto.sql;

import com.facebook.presto.sql.parser.ParsingOptions;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.tree.ComparisonExpression;
import com.facebook.presto.sql.tree.Node;
import com.facebook.presto.sql.tree.QualifiedName;
import com.facebook.presto.sql.tree.Query;
import com.facebook.presto.sql.tree.QuerySpecification;
import com.facebook.presto.sql.tree.Select;
import com.facebook.presto.sql.tree.SingleColumn;
import com.facebook.presto.sql.tree.Statement;
import com.facebook.presto.sql.tree.StringLiteral;
import com.facebook.presto.sql.tree.Table;

import com.facebook.presto.sql.tree.TableSubquery;
import com.facebook.presto.sql.tree.With;
import com.facebook.presto.sql.tree.WithQuery;
import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import java.util.Set;
import org.testng.annotations.Test;

import static com.facebook.presto.sql.AbbreviatorUtil.*;
import static com.facebook.presto.sql.parser.ParsingOptions.DecimalLiteralTreatment.*;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.assertEquals;

public class TestQueryAbbreviator {

    private static final SqlParser SQL_PARSER = new SqlParser();

    @Test
    public void testAbbreviationSimple()
    {
        String query = "select foobar from barfoo where foofoo = barbar";

        Statement root = SQL_PARSER.createStatement(query, new ParsingOptions(AS_DOUBLE));

        String abbreviation_39 = QueryAbbreviator.abbreviate(root, Optional.empty(), 39);
        assertTrue(abbreviation_39.length() <= 39);
        assertEquals(abbreviation_39, "SELECT foobar\n" + "FROM\n" + "  barfoo\n" + "WHERE ...\n");

        String abbreviation_36 = QueryAbbreviator.abbreviate(root, Optional.empty(), 36);
        assertTrue(abbreviation_36.length() <= 36);
        assertEquals(abbreviation_36, "SELECT ...\n" + "FROM\n" + "  barfoo\n" + "WHERE ...\n");

        String abbreviation_30 = QueryAbbreviator.abbreviate(root, Optional.empty(), 30);
        assertTrue(abbreviation_30.length() <= 30);
        assertEquals(abbreviation_30, "...FROM\n" + "  barfoo\n" + "WHERE ...\n");

        String abbreviation_20 = QueryAbbreviator.abbreviate(root, Optional.empty(), 20);
        assertTrue(abbreviation_20.length() <= 20);
        assertEquals(abbreviation_20, "...");
    }

    @Test
    public void testLongInExpressionList()
    {
        StringBuilder queryBuilder = new StringBuilder().append("select foobar from barfoo where foofoo not in (");
        for (int i = 0; i < 10000; i++) {
            queryBuilder.append("'abc', ");
        }

        queryBuilder.append(" 'abc')");
        String queryString = queryBuilder.toString();

        Statement statement = SQL_PARSER.createStatement(queryString, new ParsingOptions(AS_DOUBLE));
        String abbreviation = QueryAbbreviator.abbreviate(statement, Optional.empty(), 70);

        assertEquals(abbreviation, "SELECT foobar\n" + "FROM\n" + "  barfoo\n" + "WHERE (NOT (foofoo IN ...))\n");
    }


    @Test
    public void testQueryAbbreviation()
    {
          ImmutableList.Builder<String> queryBuilder = ImmutableList.builder();

          queryBuilder.add("create table foo as (with t(x) as (values 1) select x from t)")
              .add("select foobar from barfoo where foofoo = barbar group by foobar order by foobar")
              .add("select a, sum(c) as d from b")
                .add("select a, sum(c) as d from b natural join c where d not in ('e','f','g') and h > 2 group by a order by sum(c)")
                .add("Select a from b where c not in (z, (select d from e where f=g))")
                .add("show partitions from d")
              .add("select * from foo")
              .add("explain select * from foo")
              .add("explain (type distributed, format graphviz) select * from foo")
              .add("select * from foo /* end */")
              .add("/* start */ select * from foo")
              .add("/* start */ select * /* middle */ from foo /* end */")
              .add("-- start\nselect * -- junk\n-- hi\nfrom foo -- done")
              .add("select * from foo a (x, y, z)")
              .add("select *, 123, * from foo")
              .add("select show from foo")
              .add("select extract(day from x), extract(dow from x) from y")
              .add("select 1 + 13 || '15' from foo")
              .add("select x is distinct from y from foo where a is not distinct from b")
              .add("select x[1] from my_table")
              .add("select x[1][2] from my_table")
              .add("select x[cast(10 * sin(x) as bigint)] from my_table")
              .add("select * from unnest(t.my_array)")
              .add("select * from unnest(array[1, 2, 3])")
              .add("select x from unnest(array[1, 2, 3]) t(x)")
              .add("select * from users cross join unnest(friends)")
              .add("select id, friend from users cross join unnest(friends) t(friend)")
              .add("select * from unnest(t.my_array) with ordinality")
              .add("select * from unnest(array[1, 2, 3]) with ordinality")
              .add("select x from unnest(array[1, 2, 3]) with ordinality t(x)")
              .add("select * from users cross join unnest(friends) with ordinality")
              .add("select id, friend from users cross join unnest(friends) with ordinality t(friend)")
              .add("select count(*) x from src group by k, v")
              .add("select count(*) x from src group by cube (k, v)")
              .add("select count(*) x from src group by rollup (k, v)")
              .add("select count(*) x from src group by grouping sets ((k, v))")
              .add("select count(*) x from src group by grouping sets ((k, v), (v))")
              .add("select count(*) x from src group by grouping sets (k, v, k)")
              .add("select count(*) filter (where x > 4) y from t")
              .add("select sum(x) filter (where x > 4) y from t")
              .add("select sum(x) filter (where x > 4) y, sum(x) filter (where x < 2) z from t")
              .add("select sum(distinct x) filter (where x > 4) y, sum(x) filter (where x < 2) z from t")
              .add("select sum(x) filter (where x > 4) over (partition by y) z from t")
              .add("" +
                  "select depname, empno, salary\n" +
                  ", count(*) over ()\n" +
                  ", avg(salary) over (partition by depname)\n" +
                  ", rank() over (partition by depname order by salary desc)\n" +
                  ", sum(salary) over (order by salary rows unbounded preceding)\n" +
                  ", sum(salary) over (partition by depname order by salary rows between current row and 3 following)\n" +
                  ", sum(salary) over (partition by depname range unbounded preceding)\n" +
                  ", sum(salary) over (rows between 2 preceding and unbounded following)\n" +
                  "from emp"
              )
              .add("" +
                  "with a (id) as (with x as (select 123 from z) select * from x) " +
                  "   , b (id) as (select 999 from z) " +
                  "select * from a join b using (id)")
              .add("with recursive t as (select * from x) select * from t")
              .add("select * from information_schema.tables")
              .add("show catalogs")
              .add("show schemas")
              .add("show schemas from sys")
              .add("show tables")
              .add("show tables from information_schema")
              .add("show tables like '%'")
              .add("show tables from information_schema like '%'")
              .add("show partitions from foo")
              .add("show partitions from foo where name = 'foo'")
              .add("show partitions from foo order by x")
              .add("show partitions from foo limit 10")
              .add("show partitions from foo limit all")
              .add("show partitions from foo order by x desc limit 10")
              .add("show partitions from foo order by x desc limit all")
              .add("show functions")
              .add("select cast('123' as bigint), try_cast('foo' as bigint)")
              .add("select * from a.b.c")
              .add("select * from a.b.c.e.f.g")
              .add("select \"TOTALPRICE\" \"my price\" from \"$MY\"\"ORDERS\"")
              .add("select * from foo tablesample system (10+1)")
              .add("select * from foo tablesample system (10) join bar tablesample bernoulli (30) on a.id = b.id")
              .add("select * from foo tablesample system (10) join bar tablesample bernoulli (30) on not(a.id > b.id)")
              .add("create table foo as (select * from abc)")
              .add("create table if not exists foo as (select * from abc)")
              .add("create table foo with (a = 'apple', b = 'banana') as select * from abc")
              .add("create table foo comment 'test' with (a = 'apple') as select * from abc")
              .add("create table foo as select * from abc WITH NO DATA")
              .add("create table foo as (with t(x) as (values 1) select x from t)")
              .add("create table if not exists foo as (with t(x) as (values 1) select x from t)")
              .add("create table foo as (with t(x) as (values 1) select x from t) WITH DATA")
              .add("create table if not exists foo as (with t(x) as (values 1) select x from t) WITH DATA")
              .add("create table foo as (with t(x) as (values 1) select x from t) WITH NO DATA")
              .add("create table if not exists foo as (with t(x) as (values 1) select x from t) WITH NO DATA")
              .add("create table foo(a) as (with t(x) as (values 1) select x from t)")
              .add("create table if not exists foo(a) as (with t(x) as (values 1) select x from t)")
              .add("create table foo(a) as (with t(x) as (values 1) select x from t) WITH DATA")
              .add("create table if not exists foo(a) as (with t(x) as (values 1) select x from t) WITH DATA")
              .add("create table foo(a) as (with t(x) as (values 1) select x from t) WITH NO DATA")
              .add("create table if not exists foo(a) as (with t(x) as (values 1) select x from t) WITH NO DATA")
              .add("drop table foo")
              .add("insert into foo select * from abc")
              .add("delete from foo")
              .add("delete from foo where a = b")
              .add("values ('a', 1, 2.2), ('b', 2, 3.3)")
              .add("table foo")
              .add("table foo order by x limit 10")
              .add("(table foo)")
              .add("(table foo) limit 10")
              .add("(table foo limit 5) limit 10")
              .add("select * from a limit all")
              .add("select * from a order by x limit all")
              .add("select * from a union select * from b")
              .add("table a union all table b")
              .add("(table foo) union select * from foo union (table foo order by x)")
              .add("table a union table b intersect table c")
              .add("(table a union table b) intersect table c")
              .add("table a union table b except table c intersect table d")
              .add("(table a union table b except table c) intersect table d")
              .add("((table a union table b) except table c) intersect table d")
              .add("(table a union (table b except table c)) intersect table d")
              .add("table a intersect table b union table c")
              .add("table a intersect (table b union table c)")
              .add("alter table foo rename to bar")
              .add("alter table a.b.c rename to d.e.f")
              .add("alter table a.b.c rename column x to y")
              .add("alter table a.b.c add column x bigint")
              .add("alter table a.b.c drop column x")
              .add("create schema test")
              .add("create schema if not exists test")
              .add("create schema test with (a = 'apple', b = 123)")
              .add("drop schema test")
              .add("drop schema test cascade")
              .add("drop schema if exists test")
              .add("drop schema if exists test restrict")
              .add("alter schema foo rename to bar")
              .add("alter schema foo.bar rename to baz")
              .add("create table test (a boolean, b bigint, c double, d varchar, e timestamp)")
              .add("create table test (a boolean, b bigint comment 'test')")
              .add("create table if not exists baz (a timestamp, b varchar)")
              .add("create table test (a boolean, b bigint) with (a = 'apple', b = 'banana')")
              .add("create table test (a boolean, b bigint) comment 'test' with (a = 'apple')")
              .add("drop table test")
              .add("create view foo as with a as (select 123) select * from a")
              .add("create or replace view foo as select 123 from t")
              .add("drop view foo")
              .add("insert into t select * from t")
              .add("insert into t (c1, c2) select * from t")
              .add("start transaction")
              .add("start transaction isolation level read uncommitted")
              .add("start transaction isolation level read committed")
              .add("start transaction isolation level repeatable read")
              .add("start transaction isolation level serializable")
              .add("start transaction read only")
              .add("start transaction read write")
              .add("start transaction isolation level read committed, read only")
              .add("start transaction read only, isolation level read committed")
              .add("start transaction read write, isolation level serializable")
              .add("commit")
              .add("commit work")
              .add("rollback")
              .add("rollback work")
              .add("call foo()")
              .add("call foo(123, a => 1, b => 'go', 456)")
              .add("grant select on foo to alice with grant option")
              .add("grant all privileges on foo to alice")
              .add("grant delete, select on foo to public")
              .add("revoke grant option for select on foo from alice")
              .add("revoke all privileges on foo from alice")
              .add("revoke insert, delete on foo from public")
              .add("show grants on table t")
                .add("show grants on t")
                .add("show grants")
             .add("prepare p from select * from (select * from T) \"A B\"")
             .add("SELECT * FROM table1 WHERE a >= ALL (VALUES 2, 3, 4)")
              .add("SELECT * FROM table1 WHERE a <> ANY (SELECT 2, 3, 4)")
              .add("SELECT * FROM table1 WHERE a = SOME (SELECT id FROM table2)");

          List<String> queries = queryBuilder.build();

          for(String query : queries) {
              testAbbreviationLogic(query);
          }
    }

    @Test
    public void testbed()
    {
        String query = "create table foo as (with t(x) as (values 1) select x from t)";
        testPriorityAssigner(SQL_PARSER.createStatement(query, new ParsingOptions(AS_DOUBLE)));
    }


    public void testAbbreviationLogic(String query)
    {
        System.out.println(query);
        Statement root = SQL_PARSER.createStatement(query, new ParsingOptions(AS_DOUBLE));
        testPriorityAssigner(root);
        testPruning(query);
    }

    public void testPriorityAssigner(Statement root)
    {
        verifyPruningOrder(root);
        verifyPriorityValues(root);
    }

    @Test
    public void testPruning(String query)
    {
        Statement root = SQL_PARSER.createStatement(query, new ParsingOptions(AS_DOUBLE));

        // Generate a queue of candidates to prune
        Queue<QueryAbbreviator.NodeInfo> pruneOrder = QueryAbbreviator.generatePruningOrder(root);

        // Get the length of formatted sql for unpruned tree
        String formattedSql = SqlFormatter.formatSql(root, Optional.empty());
        int expectedSize = formattedSql.length();

        while(! pruneOrder.isEmpty()) {
            // Verify that the change in query length returned by pruneOneNode method is actually correct.
            int changeInSize = QueryAbbreviator.pruneOneNode(pruneOrder);
            expectedSize -= changeInSize;
            assertEquals(SqlFormatter.formatSql(root, Optional.empty()).length(), expectedSize);
        }
    }

    private void verifyPriorityValues(Statement root)
    {
        Queue<QueryAbbreviator.NodeInfo> pruningCandidates = QueryAbbreviator.generatePruningOrder(root);

        Map<Node, QueryAbbreviator.NodeInfo> nodeInfoMap = new IdentityHashMap<>();

        while(!pruningCandidates.isEmpty()) {
              QueryAbbreviator.NodeInfo currentNodeInfo = pruningCandidates.poll();
              nodeInfoMap.put(currentNodeInfo.getNode(), currentNodeInfo);
        }

        verifyPriorityRecursive(root, nodeInfoMap, 1, 1.0);

//        while(!pruningCandidates.isEmpty()) {
//            QueryAbbreviator.NodeInfo nodeInfo = pruningCandidates.poll();
//
//            for (Node child : nodeInfo.getNode().getChildren()) {
//                childToParentMap.put(child, nodeInfo);
//            }
//
//            pruningCandidatesCopy.add(nodeInfo);
//        }
//
//        while(!pruningCandidatesCopy.isEmpty()) {
//
//            QueryAbbreviator.NodeInfo selfInfo = pruningCandidatesCopy.poll();
//            QueryAbbreviator.NodeInfo parentInfo = childToParentMap.get(selfInfo.getNode());
//
//            if (parentInfo != null) {
//                assertEquals(selfInfo.getChildPriorityVal(),
//                    parentInfo.getChildPriorityVal() / selfInfo.getNode().getChildren().size());
//                assertEquals(selfInfo.getLevel(), parentInfo.getLevel() + 1);
//                assertTrue(selfInfo.getIndent() >= parentInfo.getIndent());
//            }
//            else {
//                assertEquals(selfInfo.getChildPriorityVal(), 1.0 / selfInfo.getNode().getChildren().size());
//            }
//        }
    }

    private void verifyPriorityRecursive(Node node, Map<Node,QueryAbbreviator.NodeInfo> nodeInfoMap, int expectedLevel, double expectedPriority)
    {
        if(nodeInfoMap.containsKey(node)) {

            QueryAbbreviator.NodeInfo nodeInfo = nodeInfoMap.get(node);
            //System.out.println((node) + ", " + node.getClass() + ", child priority: " + nodeInfo.getChildPriorityVal() + ", level: " + nodeInfo.getLevel());
            assertEquals(nodeInfo.getLevel(), expectedLevel);
            assertEquals(nodeInfo.getChildPriorityVal(), expectedPriority / Math.max(node.getChildren().size(),1));
        }

        if(node instanceof Query) {
            handleQuery((Query) node, nodeInfoMap, expectedLevel, expectedPriority);
        }
        else {
            int childLevel = expectedLevel + 1;
            double childPriority = expectedPriority / Math.max(node.getChildren().size(), 1);

            for (Node child : node.getChildren()) {
                verifyPriorityRecursive(child, nodeInfoMap, childLevel, childPriority);
            }
        }
    }

    private void handleQuery(Query node, Map<Node,QueryAbbreviator.NodeInfo> nodeInfoMap, int expectedLevel, double expectedPriority)
    {
        int childLevel = expectedLevel + 1;
        double childPriority = expectedPriority / Math.max(node.getChildren().size(),1);

        if (node.getWith().isPresent()) {

            With with = node.getWith().get();
            Iterator<WithQuery> queries = with.getQueries().iterator();

            int withQueryLevel = childLevel + 1;
            double withQueryPriority = childPriority / Math.max(with.getQueries().size(), 1);
            while (queries.hasNext()) {
                WithQuery query = queries.next();
                verifyPriorityRecursive(new TableSubquery(query.getQuery()), nodeInfoMap, withQueryLevel, withQueryPriority);
            }
        }

        verifyPriorityRecursive(node.getQueryBody(), nodeInfoMap, childLevel, childPriority);
        if (node.getOrderBy().isPresent()) {
            verifyPriorityRecursive(node.getOrderBy().get(), nodeInfoMap, childLevel, childPriority);
        }
    }

    private void verifyPruningOrder(Statement root) {

        Queue<QueryAbbreviator.NodeInfo> pruningCandidates = QueryAbbreviator.generatePruningOrder(root);
        Set<Node> alreadyPopped = new HashSet<>();

        while(!pruningCandidates.isEmpty()) {

            QueryAbbreviator.NodeInfo nodeInfo = pruningCandidates.poll();
            Node currentNode = nodeInfo.getNode();

            alreadyPopped.add(currentNode);

            // Verify that the node added to pruningCandidates is indeed allowed to be pruned
            assertTrue(isAllowedToBePruned(nodeInfo.getNode()));

            // Verify that all the eligible nodes in the subtree rooted at currentNode have already been popped from the queue
            assertTrue(verifySubTreeAlreadyConsidered(currentNode, alreadyPopped));
        }
    }

    private boolean verifySubTreeAlreadyConsidered(Node node, Set<Node> alreadyPopped)
    {
        if(isAllowedToBePruned(node) && !alreadyPopped.contains(node)) {
            return false;
        }

        for(Node child : node.getChildren()) {

          if(!verifySubTreeAlreadyConsidered(child, alreadyPopped)) {
                return false;
            }
        }

        return true;
    }
}
