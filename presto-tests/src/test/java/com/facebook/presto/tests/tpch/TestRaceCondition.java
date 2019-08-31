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
package com.facebook.presto.tests.tpch;

import com.facebook.presto.Session;
import com.facebook.presto.execution.RemoteTask;
import com.facebook.presto.execution.SqlStageExecution;
import com.facebook.presto.execution.StageState;
import com.facebook.presto.execution.scheduler.ExecutionPolicy;
import com.facebook.presto.execution.scheduler.ExecutionSchedule;
import com.facebook.presto.execution.scheduler.NodeMapSupplierProvider;
import com.facebook.presto.execution.scheduler.TestingNodeMapSupplierProvider;
import com.facebook.presto.spi.Node;
import com.facebook.presto.sql.planner.PlanFragment;
import com.facebook.presto.sql.planner.plan.JoinNode;
import com.facebook.presto.sql.planner.plan.PlanFragmentId;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.planner.plan.RemoteSourceNode;
import com.facebook.presto.testing.MaterializedResult;
import com.facebook.presto.testing.MaterializedRow;
import com.facebook.presto.tests.DistributedQueryRunner;
import com.facebook.presto.tpch.TpchPlugin;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.inject.Binder;
import com.google.inject.Inject;
import com.google.inject.Module;
import com.google.inject.Scopes;
import com.google.inject.multibindings.MapBinder;
import io.airlift.units.DataSize;
import org.testng.annotations.Test;

import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static com.facebook.presto.execution.StageState.RUNNING;
import static com.facebook.presto.execution.StageState.SCHEDULED;
import static com.facebook.presto.execution.buffer.BufferState.FLUSHING;
import static com.facebook.presto.sql.planner.plan.JoinNode.DistributionType.PARTITIONED;
import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.inject.multibindings.MapBinder.newMapBinder;
import static java.util.Objects.requireNonNull;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class TestRaceCondition
{
    @Test(timeOut = 300_000)
    public void testRace() throws Exception
    {
        Session session = testSessionBuilder().build();

        // RaceConditionExecutionPolicyModule schedules stages in a specific order.

        // TestingNodeMapSupplierProvider allows to disable and re-enable certain nodes for scheduling splits. This is
        // useful to control task scheduling.

        DistributedQueryRunner queryRunner = DistributedQueryRunner.builder(session)
                .setNodeCount(2)
                .setAdditionalModule(new RaceConditionExecutionPolicyModule(), true)
                .setAdditionalModule(binder -> binder.bind(NodeMapSupplierProvider.class).to(TestingNodeMapSupplierProvider.class).in(Scopes.SINGLETON), false)
                .setSingleExtraProperty("sink.max-buffer-size", new DataSize(1024, DataSize.Unit.BYTE).toString())
                .build();

        queryRunner.installPlugin(new TpchPlugin());
        queryRunner.createCatalog("tpch", "tpch", ImmutableMap.of("tpch.splits-per-node", "1"));

        Session sessionWithRacyScheduling = testSessionBuilder()
                .setSystemProperty("execution_policy", "test-race-condition")
                .setSystemProperty("task_concurrency", "1")
                .build();

        // Left side of the join contains only 1 row, for orderkey=1. The orderkey filter also includes -7 to make sure that
        // the left scan indeed ends up being another stage (rather than being in the same stage as the join), which is required for reproducing this bug.
        MaterializedResult result = queryRunner.execute(sessionWithRacyScheduling, "SELECT b.name FROM tpch.tiny.orders a JOIN tpch.sf10.customer b on a.custkey = b.custkey where a.orderkey in (1,-7)");

        for (MaterializedRow row : result.getMaterializedRows()) {
            System.out.println(row.toString());
        }
    }

    public static class RaceConditionExecutionPolicyModule
            implements Module
    {
        @Override
        public void configure(Binder binder)
        {
            MapBinder<String, ExecutionPolicy> executionPolicyBinder = newMapBinder(binder, String.class, ExecutionPolicy.class);
            executionPolicyBinder.addBinding("test-race-condition").to(RaceConditionExecutionPolicy.class);
        }
    }

    public static class RaceConditionExecutionPolicy
            implements ExecutionPolicy
    {
        NodeMapSupplierProvider provider;

        @Inject
        public RaceConditionExecutionPolicy(NodeMapSupplierProvider provider)
        {
            this.provider = requireNonNull(provider, "provider is null");
            assertTrue(provider instanceof TestingNodeMapSupplierProvider);
        }

        @Override
        public ExecutionSchedule createExecutionSchedule(Collection<SqlStageExecution> stages)
        {
            return new TestingRaceConditionScheduler(new HashSet<>(stages), (TestingNodeMapSupplierProvider) provider);
        }
    }

    public static class TestingRaceConditionScheduler
            implements ExecutionSchedule
    {
        private Set<SqlStageExecution> inconsequentialStages;
        private SqlStageExecution probeSourceStage;
        private SqlStageExecution buildSourceStage;
        private SqlStageExecution joinStage;

        private boolean allDone;
        private TestingNodeMapSupplierProvider nodeMapSupplierProvider;
        private final Set<Node> allNodes;

        public TestingRaceConditionScheduler(Set<SqlStageExecution> stages, TestingNodeMapSupplierProvider nodeMapSupplierProvider)
        {
            // Get all join nodes in the execution plan
            Map<PlanFragmentId, List<PlanNode>> joinNodes = getJoinNodesWithFragments(stages.stream().map(execution -> execution.getFragment()).collect(Collectors.toList()));

            checkArgument(joinNodes.size() == 1, "Exactly one fragment should have a join node");

            PlanFragmentId joinStageId = Iterables.getOnlyElement(joinNodes.keySet());
            checkArgument(joinNodes.get(joinStageId).size() == 1, "The fragment should have exactly one join node");

            JoinNode joinNode = (JoinNode) joinNodes.get(joinStageId).get(0);
            assertEquals(joinNode.getDistributionType().get(), PARTITIONED);

            // Probe side of the join should get data from exactly one remote source
            RemoteSourceNode probeRemoteSource = (RemoteSourceNode) Iterables.getOnlyElement(findNodes(joinNode.getLeft(), x -> x instanceof RemoteSourceNode));
            PlanFragmentId probeSourceFragmentId = Iterables.getOnlyElement(probeRemoteSource.getSourceFragmentIds());

            // Build side of the join should get data from exactly one remote source
            RemoteSourceNode buildRemoteSource = (RemoteSourceNode) Iterables.getOnlyElement(findNodes(joinNode.getRight(), x -> x instanceof RemoteSourceNode));
            PlanFragmentId buildSourceFragmentId = Iterables.getOnlyElement(buildRemoteSource.getSourceFragmentIds());

            probeSourceStage = stages.stream()
                            .filter(s -> s.getFragment().getId().equals(probeSourceFragmentId))
                            .findAny()
                            .get();

            buildSourceStage = stages.stream()
                    .filter(s -> s.getFragment().getId().equals(buildSourceFragmentId))
                    .findAny()
                    .get();

            joinStage = stages.stream()
                    .filter(s -> s.getFragment().getId().equals(joinStageId))
                    .findAny()
                    .get();

            // Stages that are not relevant for this unit test
            inconsequentialStages = new HashSet<>(stages);
            inconsequentialStages.remove(probeSourceStage);
            inconsequentialStages.remove(buildSourceStage);
            inconsequentialStages.remove(joinStage);

            this.nodeMapSupplierProvider = nodeMapSupplierProvider;

            // Make sure that 2 nodes are visible
            Set<Node> allNodes = nodeMapSupplierProvider.getCurrentlyVisibleNodeMap().getNodesByHost().values().stream().collect(Collectors.toSet());
            assertEquals(allNodes.size(), 2);
            this.allNodes = allNodes;
        }

        @Override
        public Set<SqlStageExecution> getStagesToSchedule()
        {
            // Schedule inconsequential stages first

            for (Iterator<SqlStageExecution> iterator = inconsequentialStages.iterator(); iterator.hasNext(); ) {
                StageState state = iterator.next().getState();
                if (state == SCHEDULED || state == RUNNING || state.isDone()) {
                    iterator.remove();
                }
            }

            if (!inconsequentialStages.isEmpty()) {
                return inconsequentialStages;
            }

            // Schedule probe stage once inconsequentialStages are done

            StageState probeStageState = probeSourceStage.getState();
            boolean probeStageScheduled = (probeStageState == SCHEDULED || probeStageState == RUNNING || probeStageState.isDone());

            if (!probeStageScheduled) {
                return ImmutableSet.of(probeSourceStage);
            }

            // Check that the probe side stage is ready with the data. If not, then don't schedule anything for now.

            boolean isProbeStageFlushingOrComplete = probeStageState.isDone() ||
                    (probeSourceStage.hasTasks() &&
                    probeSourceStage.getAllTasks().stream().allMatch(x -> x.getTaskInfo().getOutputBuffers().getState().equals(FLUSHING)));

            if (!isProbeStageFlushingOrComplete) {
                return ImmutableSet.of();
            }

            // Schedule the stage involving the Join, once the probeSourceStage has all the data

            StageState joinStageState = joinStage.getState();
            boolean joinStageScheduled = (joinStageState == SCHEDULED || joinStageState == RUNNING || joinStageState.isDone());

            if (!joinStageScheduled) {
                return ImmutableSet.of(joinStage);
            }

            // Some context for reference: the join task has 3 pipelines.
            //     pipeline0(ExchangeOperator->LocalExchangeSinkOperator)
            //     pipeline1(LocalExchangeSourceOperator->HashBuilderOperator)
            //     pipeline2(ExchangeOperator->LookupJoinOperator->TaskOutputOperator)

            // One of the two tasks is going to have 0 rows, because of the way the query is structured. For this task,
            // the pipeline with the LookupJoinOperator (i.e. pipeline2) should collapse. We make sure this happens
            // ***BEFORE*** scheduling the build stage.
            //
            // Reason: Scheduling the build side stage can cause pipeline0 or pipeline1 in join stage start before the probe-side
            // pipeline2. This can block pipeline0 and pipeline1 on ExchangeOperator and LocalExchangeSourceOperator respectively.
            // If pipeline2 finishes after that, it will signal HashBuilderOperator to collapse the NEXT time isFinished is invoked on it.
            // But it will still NOT unblock pipeline1, since pipeline1 is still blocked on LocalExchangeSourceOperator. @dain talks
            // about something like this here: https://github.com/prestodb/presto/pull/10604#issuecomment-404556571 but not sure
            // if it's the same thing.

            // The check here in atLeastOneDriverFinished is a loose one, only looking if at least one driver has finished.
            // We assume that it's the one that processes the `pipeline2`. Also, we only have one driver per pipeline because task concurrency is 1.

            boolean lookupJoinOperatorPipelineFinished = atLeastOneDriverFinished(joinStage);

            if (!lookupJoinOperatorPipelineFinished) {
                return ImmutableSet.of();
            }

            // The next step is to trigger the completion of one join task. This happens if pipeline0 and pipeline1 are unblocked.
            // We do this by scheduling ONLY one join task. We want to save the second one for for later, to demonstrate buffers not
            // being closed.

            boolean oneBuildTaskScheduled = buildSourceStage.hasTasks();
            if (!oneBuildTaskScheduled) {
                // TpchSplit is not remotely accessible. By setting a worker node as invisible, we ensure that the split scheduler
                // does not find that node and does not schedule a task on it. Hacky, I know. Need a better strategy (involving
                // a mock TaskManager may be) if we want to write a check-in worthy unit test.
                Node candidate = allNodes.stream().filter(x -> !x.isCoordinator()).findAny().get();
                nodeMapSupplierProvider.setInvisible(candidate);

                return ImmutableSet.of(buildSourceStage);
            }

            // Now, wait for that one join task to finish.

            boolean oneJoinTaskFinished = (joinStage.getAllTasks().stream()
                    .map(t -> t.getTaskStatus().getState())
                    .filter(s -> s.isDone())
                    .collect(Collectors.toList())
                    .size() >= 1);

            if (!oneJoinTaskFinished) {
                return ImmutableSet.of();
            }

            /*

            At this point, one join task has finished even before one of the build tasks was scheduled. The build task
            that we schedule below will fill up its buffer corresponding to the already closed join task and hang. We lowered
            the buffer size to 1MB to achieve this reliably.

            From one of the runs of this code, output buffer of the LAST scheduled build task looks like the following:

                "buffers" : [ {
                  "bufferId" : 0,
                  "finished" : false,
                  "bufferedPages" : 2,
                  "pagesSent" : 0,
                  "pageBufferInfo" : {
                    "partition" : 0,
                    "bufferedPages" : 2,
                    "bufferedBytes" : 1048987,
                    "rowsAdded" : 25579,
                    "pagesAdded" : 2
                  }
                }, {
                  "bufferId" : 1,
                  "finished" : false,
                  "bufferedPages" : 0,
                  "pagesSent" : 0,
                  "pageBufferInfo" : {
                    "partition" : 1,
                    "bufferedPages" : 0,
                    "bufferedBytes" : 0,
                    "rowsAdded" : 0,
                    "pagesAdded" : 0
                  }
                } ]

            Here, the downstream task that is supposed to read buffer with id 0 (task-0 in joinstage) is already finished.
            As we see, bufferredBytes for bufferid=0 is >1MB, which causes this task to halt because of full buffers.
            task-1 of joinstage is all caught up and waiting for new data, but it will never receive it since buffer for a
            different partition (bufferid 0) has filled up the whole buffer.

             That is what causes the query to hang!!

             */

            // Schedule the last remaining stage

            StageState buildState = buildSourceStage.getState();
            boolean buildStageCompletelyScheduled = (buildState == SCHEDULED || buildState == RUNNING || buildState.isDone());

            if (!buildStageCompletelyScheduled) {
                // Restoring all nodes. Now we also want the other build stage task to get scheduled.
                nodeMapSupplierProvider.restoreAll();
                return ImmutableSet.of(buildSourceStage);
            }

            // Done with scheduling !
            allDone = true;
            return ImmutableSet.of();
        }

        private boolean atLeastOneDriverFinished(SqlStageExecution stage)
        {
            if (stage.getState().isDone()) {
                return true;
            }

            for (RemoteTask task : stage.getAllTasks()) {
                if (task.getTaskInfo().getStats().getCompletedDrivers() >= 1) {
                    return true;
                }
            }
            return false;
        }

        @Override
        public boolean isFinished()
        {
            return allDone;
        }
    }

    /**
     * Find join nodes from all fragments
     */
    private static Map<PlanFragmentId, List<PlanNode>> getJoinNodesWithFragments(List<PlanFragment> fragments)
    {
        ImmutableMap.Builder<PlanFragmentId, List<PlanNode>> joinNodesWithFragmentsBuilder = ImmutableMap.builder();

        fragments.forEach(fragment -> {
            List<PlanNode> joinNodes = findNodes(fragment.getRoot(), x -> x instanceof JoinNode);

            if (!joinNodes.isEmpty()) {
                joinNodesWithFragmentsBuilder.put(fragment.getId(), joinNodes);
            }
        });

        return joinNodesWithFragmentsBuilder.build();
    }

    // Find nodes in a subtree satisfying the given condition
    private static List<PlanNode> findNodes(PlanNode node, Predicate<PlanNode> condition)
    {
        ImmutableList.Builder<PlanNode> qualifyingNodesBuilder = ImmutableList.builder();
        fillQualifyingNodes(node, condition, qualifyingNodesBuilder);
        return qualifyingNodesBuilder.build();
    }

    private static void fillQualifyingNodes(PlanNode node, Predicate<PlanNode> condition, ImmutableList.Builder<PlanNode> qualifyingNodes)
    {
        if (condition.test(node)) {
            qualifyingNodes.add(node);
        }
        node.getSources().forEach(x -> fillQualifyingNodes(x, condition, qualifyingNodes));
    }
}
