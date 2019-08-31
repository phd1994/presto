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
package com.facebook.presto.execution.scheduler;

import com.facebook.presto.metadata.InternalNodeManager;
import com.facebook.presto.spi.HostAddress;
import com.facebook.presto.spi.Node;
import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableSetMultimap;

import javax.annotation.PostConstruct;
import javax.inject.Inject;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import static com.facebook.presto.spi.NodeState.ACTIVE;
import static com.google.common.collect.ImmutableSet.toImmutableSet;

public class TestingNodeMapSupplierProvider
        implements NodeMapSupplierProvider
{
    private AtomicReference<NodeMap> globalNodeMap = new AtomicReference<>();
    private AtomicReference<NodeMap> visibleGlobalNodeMap = new AtomicReference<>();

    public InternalNodeManager nodeManager;

    @Inject
    public TestingNodeMapSupplierProvider(InternalNodeManager nodeManager)
    {
        this.nodeManager = nodeManager;
    }

    @PostConstruct
    public void initialize()
    {
        globalNodeMap.compareAndSet(null, computeNodeMap());
        visibleGlobalNodeMap.compareAndSet(null, globalNodeMap.get());
    }

    public void refreshGlobalNodeMap()
    {
        globalNodeMap.set(computeNodeMap());
    }

    @Override
    public Supplier<NodeMap> getNodeMapSupplier(Supplier<NodeMap> ignored)
    {
        return () -> { return visibleGlobalNodeMap.get(); };
    }

    private NodeMap computeNodeMap()
    {
        ImmutableSetMultimap.Builder<HostAddress, Node> byHostAndPort = ImmutableSetMultimap.builder();
        ImmutableSetMultimap.Builder<InetAddress, Node> byHost = ImmutableSetMultimap.builder();
        ImmutableSetMultimap.Builder<NetworkLocation, Node> workersByNetworkPath = ImmutableSetMultimap.builder();

        Set<Node> nodes = nodeManager.getNodes(ACTIVE);

        Set<String> coordinatorNodeIds = nodeManager.getCoordinators().stream()
                .map(Node::getNodeIdentifier)
                .collect(toImmutableSet());

        for (Node node : nodes) {
            byHostAndPort.put(node.getHostAndPort(), node);
            InetAddress host;
            try {
                host = InetAddress.getByName(node.getHttpUri().getHost());
            }
            catch (UnknownHostException e) {
                throw new RuntimeException(e);
            }
            byHost.put(host, node);
        }

        return new NodeMap(byHostAndPort.build(), byHost.build(), workersByNetworkPath.build(), coordinatorNodeIds);
    }

    public void setInvisible(Node targetNode)
    {
        NodeMap globalMap = globalNodeMap.get();

        ImmutableSetMultimap.Builder<HostAddress, Node> byHostAndPort = ImmutableSetMultimap.builder();
        ImmutableSetMultimap.Builder<InetAddress, Node> byHost = ImmutableSetMultimap.builder();
        ImmutableSetMultimap.Builder<NetworkLocation, Node> workersByNetworkPath = ImmutableSetMultimap.builder();

        Set<String> coordinatorNodeIds = globalMap.getCoordinatorNodeIds().stream()
                .filter(nodeId -> !targetNode.getNodeIdentifier().equals(nodeId))
                .collect(toImmutableSet());

        for (Map.Entry<InetAddress, Node> entry : globalMap.getNodesByHost().entries()) {
            if (!entry.getValue().equals(targetNode)) {
                byHost.put(entry);
            }
        }

        for (Map.Entry<HostAddress, Node> entry : globalMap.getNodesByHostAndPort().entries()) {
            if (!entry.getValue().equals(targetNode)) {
                byHostAndPort.put(entry);
            }
        }

        for (Map.Entry<NetworkLocation, Node> entry : globalMap.getWorkersByNetworkPath().entries()) {
            if (!entry.getValue().equals(targetNode)) {
                workersByNetworkPath.put(entry);
            }
        }

        NodeMap newMap = new NodeMap(byHostAndPort.build(), byHost.build(), workersByNetworkPath.build(), coordinatorNodeIds);
        visibleGlobalNodeMap.set(newMap);
    }

    public void restoreAll()
    {
        visibleGlobalNodeMap.set(globalNodeMap.get());
    }

    public NodeMap getCurrentlyVisibleNodeMap()
    {
        return new NodeMap(visibleGlobalNodeMap.get());
    }
}
