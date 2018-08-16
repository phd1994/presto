/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.sql;

import com.facebook.presto.sql.tree.AstVisitor;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.Lateral;
import com.facebook.presto.sql.tree.Node;

import com.facebook.presto.sql.tree.Prepare;
import com.facebook.presto.sql.tree.TableSubquery;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.Queue;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;
import static com.facebook.presto.sql.AbbreviatorUtil.isAllowedToBePruned;

public final class QueryAbbreviator
{
    private static final String PRUNED_MARKER = "...";

    private QueryAbbreviator() {}

    public static String abbreviate(Node root, Optional<List<Expression>> parameters, int threshold)
    {
        requireNonNull(root, "root is null");
        checkArgument(threshold >= 0, "threshold is < 0");

        // Don't try pruning if threshold is smaller than the shortest possible string after abbreviation
        if (threshold < PRUNED_MARKER.length()) {
            return PRUNED_MARKER.substring(0, threshold);
        }

        // Compute priorities and generate an order of candidates to prune
        Queue<NodeInfo> pruningOrder = generatePruningOrder(root);

//        while(pruningOrder.isEmpty() == false) {
//            NodeInfo nodeInfo = pruningOrder.poll();
//            System.out.println(1.0/nodeInfo.childPriorityVal + ", " + nodeInfo.level + ", " + nodeInfo.getNode().getClass());
//        }

        // Prune the query tree.
        pruneQueryTree(root, pruningOrder, threshold);

        // construct and return formatted string for pruned tree
        return SqlFormatter.formatSql(root, Optional.empty());
    }

    static Queue<NodeInfo> generatePruningOrder(Node root) {

        PruningContext pruningContext = new PruningContext(new ArrayList<>(), 1,1, 0);
        new PriorityGenerator().process(root, pruningContext);

        List<NodeInfo> pruningCandidatesUnordered = pruningContext.getNonLeafNodesList();

        pruningCandidatesUnordered.sort((o1, o2) -> {

            if (o1.childPriorityVal < o2.childPriorityVal) {
                return -1;
            }

            if (o1.childPriorityVal == o2.childPriorityVal) {

                if (o1.level > o2.level) {
                    return -1;
                }

                if (o1.level < o2.level) {
                    return 1;
                }

                return ((Integer) o1.getNode().hashCode()).compareTo(o2.getNode().hashCode());
            }

            return 1;
        });

        Queue<NodeInfo> pruningOrder = new LinkedList<>();

        for(int i = 0; i < pruningCandidatesUnordered.size(); i++) {
            pruningOrder.add(pruningCandidatesUnordered.get(i));
        }

        return pruningOrder;
    }

    /**
     * This method keeps pruning the nodes from the query tree one-by-one until
     *      (1) we achieve the required query size OR
     *      (2) the root itself is pruned
     * If threshold value is <= PRUNED_MARKER.length(), it is possible that generated sql from
     * the pruned tree has length greater than threshold.
     *
     * @param root - root of the query tree
     * @param pruningOrder - queue of candidate nodes to prune, ordered according to their priorities.
     * @param threshold -
     */
    private static void pruneQueryTree(Node root, Queue<NodeInfo> pruningOrder, int threshold)
    {
        String originalQuery = SqlFormatter.formatSql(root, Optional.empty());
        int currentSize = originalQuery.length();

        while(currentSize > threshold) {
            try {
                int reduction = pruneOneNode(pruningOrder);
                currentSize -= reduction;
                printStuff(currentSize, root);
            }
            catch (NoSuchElementException e) {
                break;
            }
        }
    }

    public static void printStuff(int currentSize, Node root)
    {
        System.out.println("-----------------\nExpected size: " + currentSize);
        String queryNow = SqlFormatter.formatSql(root, Optional.empty());
        System.out.println("Actual size: " + queryNow.length());
        System.out.println("-----------------\nQuery String now: " + queryNow);
    }

    /**
     * This method prunes the node in front of the queue.
     * Throws NoSuchElementException if the queue is empty.
     *
     * @param pruningOrder - queue of candidate nodes to prune, ordered according to their priorities.
     * @return change in the query length after pruning one node.
     */
    static int pruneOneNode(Queue<NodeInfo> pruningOrder)
    {
        NodeInfo nodeInfo = pruningOrder.remove();
        return prune(nodeInfo.getNode(), nodeInfo.getIndent());
    }

    private static int prune(Node node, int indent)
    {
        // Formatted Sql for unpruned node
        String currentNodeSql = SqlFormatter.formatSql(node, Optional.empty(), indent);

        // Formatted Sql after pruning
        String prunedNodeSql = SqlFormatter.applyIndent(indent, PRUNED_MARKER);

        // Change in query length
        int changeInQueryLength = currentNodeSql.length() - prunedNodeSql.length();

        // Mark the node pruned and set PRUNED_MARKER
        node.setPruned(PRUNED_MARKER);

        return changeInQueryLength;
    }

    private static class PriorityGenerator
        extends AstVisitor<Void, PruningContext>
    {
        @Override
        protected Void visitNode(Node node, PruningContext context)
        {
            int numChildren = node.getChildren().size();

            double childPriority;
            if(numChildren == 0) {
                childPriority = context.getCurrentPriority();
            }
            else {
                childPriority = context.getCurrentPriority() / numChildren;
            }

            // Add NodeInfo object to the priority queue
            NodeInfo nodeInfo = new NodeInfo(
                                        node,
                                        childPriority,
                                        context.getCurrentLevel(),
                                        context.getCurrentIndent()
                                    );

            if(isAllowedToBePruned(node)) {
                context.getNonLeafNodesList().add(nodeInfo);
            }

            // compute indent value for children
            int childIndent = getChildIndent(context.getCurrentIndent(), node);

            // Generate child context
            PruningContext childContext = new PruningContext(
                                                    context.getNonLeafNodesList(),
                                                    childPriority,
                                                    context.getCurrentLevel() + 1,
                                                    childIndent
                                                );

            // Process children
            for(Node child : node.getChildren()) {
                process(child, childContext);
            }

            return null;
        }

        private static int getChildIndent(int indent, Node node)
        {
            Set<Class> indentIncrementors = new HashSet<>(Arrays.asList(Prepare.class, TableSubquery.class, Lateral.class));

            if(indentIncrementors.contains(node.getClass())) {
                return indent + 1;
            }
            return indent;
        }
    }

    static class NodeInfo
    {
        private final Node node;
        private final double childPriorityVal;
        private final int level;
        private final int indent;

        public Node getNode() {
            return node;
        }

        public double getChildPriorityVal() {
            return childPriorityVal;
        }

        public int getLevel() {
            return level;
        }

        public int getIndent() {
            return indent;
        }

        public NodeInfo(Node node, double priorityVal,  int level, int indent)
        {
            this.node = node;
            this.level = level;
            this.childPriorityVal = priorityVal;
            this.indent = indent;
        }
    }

    static class PruningContext
    {
        private final List<NodeInfo> nonLeafNodesList;
        private double currentPriority;
        private int currentLevel;
        private int currentIndent;

        public PruningContext(List<NodeInfo> nonLeafNodesList, double currentPriority, int currentLevel, int currentIndent)
        {
            this.nonLeafNodesList = nonLeafNodesList;
            this.currentPriority = currentPriority;
            this.currentLevel = currentLevel;
            this.currentIndent = currentIndent;
        }

        public List<NodeInfo> getNonLeafNodesList()
        {
            return this.nonLeafNodesList;
        }

        public double getCurrentPriority()
        {
            return this.currentPriority;
        }

        public int getCurrentLevel()
        {
            return this.currentLevel;
        }

        public int getCurrentIndent()
        {
            return this.currentIndent;
        }
    }
}
