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
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.PriorityQueue;
import java.util.Set;

public final class QueryAbbreviator
{
    private static final String PRUNED_MARKER = "...";

    private QueryAbbreviator() {}

    public static String abbreviate(Node root, Optional<List<Expression>> parameters, int threshold)
    {
        // Compute priorities and generate a heap for candidates to prune
        PruningContext pruningContext = new PruningContext(getPriorityQueue(), 1,1, 0);
        new PriorityGenerator().process(root, pruningContext);

        // Prune the tree such that generated query length <= threshold
        PriorityQueue<NodeInfo> pruningCandidateInfos = pruningContext.getNonLeafNodes();
        pruneQueryTree(root, pruningCandidateInfos, threshold);

        // return the formatted string for pruned tree
        return SqlFormatter.formatSql(root, Optional.empty());
    }

    private static void pruneQueryTree(Node root, PriorityQueue<NodeInfo> nodeInfos, int threshold)
    {
        String originalQuery = SqlFormatter.formatSql(root, Optional.empty());
        int currentSize = originalQuery.length();

        // Keep pruning nodes one-by-one until (1) we achieve the required query size  OR (2) the whole tree is pruned
        while(currentSize >= threshold) {
            try {
                int reduction = pruneOneNode(nodeInfos);
                currentSize -= reduction;
                printStuff(currentSize, root);
            }
            catch (IllegalArgumentException e) {
                System.out.println(e.getMessage());
                break;
            }
        }
    }

    public static void printStuff(int currentSize, Node root)
    {
        System.out.println("-----------------\nExpected size: " + currentSize);
        String queryNow = SqlFormatter.formatSql(root, Optional.empty());
        System.out.println("Expected size: " + queryNow.length());
        System.out.println("-----------------\nQuery String now: " + queryNow);
    }

    private static int pruneOneNode(PriorityQueue<NodeInfo> nodeInfos)
    {
        if (nodeInfos.size() == 0) {
            throw new IllegalArgumentException("Priority queue of pruning candidates is empty");
        }

        NodeInfo nodeInfo = nodeInfos.poll();
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

            // Only non-leaf nodes are added to the priority queue of pruning candidates
            if (numChildren > 0) {

                // Compute the priority value for this node's children (all children have the same priority)
                double childPriority = context.getCurrentPriority() / numChildren;

                // Add NodeInfo object to the priority queue
                NodeInfo nodeInfo = new NodeInfo(
                                            node,
                                            childPriority,
                                            context.getCurrentLevel(),
                                            context.getCurrentIndent()
                                        );
                context.getNonLeafNodes().add(nodeInfo);

                // compute indent value for children
                int childIndent = getChildIndent(context.getCurrentIndent(), node);

                // Generate child context
                PruningContext childContext = new PruningContext(
                                                        context.getNonLeafNodes(),
                                                        childPriority,
                                                        context.getCurrentLevel() + 1,
                                                        childIndent
                                                    );

                // Process children
                for(Node child : node.getChildren()) {
                    process(child, childContext);
                }
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

    private static class NodeInfo
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

    private static PriorityQueue<NodeInfo> getPriorityQueue()
    {
        return new PriorityQueue<>(10, (o1, o2) -> {

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
    }

    private static class PruningContext
    {
        private final PriorityQueue<NodeInfo> nonLeafNodes;
        private double currentPriority;
        private int currentLevel;
        private int currentIndent;

        public PruningContext(PriorityQueue<NodeInfo> nonleafNodes, double currentPriority, int currentLevel, int currentIndent)
        {
            this.nonLeafNodes = nonleafNodes;
            this.currentPriority = currentPriority;
            this.currentLevel = currentLevel;
            this.currentIndent = currentIndent;
        }

        public PriorityQueue<NodeInfo> getNonLeafNodes()
        {
            return this.nonLeafNodes;
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
