#include "../include/differential_privacy/base.hpp"

#include "analysis.pb.h"
#include <queue>

// Uncomment to force error if protobuf versions mismatch
//GOOGLE_PROTOBUF_VERIFY_VERSION;

std::set<unsigned int> getSinks(const burdock::Analysis& analysis) {
    auto nodeIds = std::set<unsigned int>();
    for (const auto& nodePair : analysis.graph())
        nodeIds.insert(nodePair.first);

    for (const auto& nodePair : analysis.graph())
        for (const auto& argumentPair : nodePair.second.arguments())
            nodeIds.erase(argumentPair.second.source_node_id());

    return nodeIds;
}

std::set<unsigned int> getSources(const burdock::Analysis& analysis) {
    auto nodeIds = std::set<unsigned int>();
    for (const auto& nodePair : analysis.graph()) {
        if (nodePair.second.arguments_size() > 0) continue;
        nodeIds.insert(nodePair.first);
    }
    return nodeIds;
}

std::set<unsigned int> getReleaseNodes(burdock::Analysis analysis) {

    std::set<unsigned int> releaseNodeIds;
    auto sinkIds = getSinks(analysis);
    std::queue<unsigned int, std::deque<unsigned int>> nodeQueue(
            std::deque<unsigned int>(sinkIds.begin(), sinkIds.end()));

    auto graph = *analysis.mutable_graph();
    while (!nodeQueue.empty()) {
        unsigned int nodeId = nodeQueue.front();
        nodeQueue.pop();

        burdock::Component component = graph[nodeId];
        if (isPrivatizer(component))
            releaseNodeIds.insert(nodeId);
        else
            for (const auto& argument : component.arguments())
                nodeQueue.push(argument.second.source_node_id());
    }

    return releaseNodeIds;
}

bool isPrivatizer(const burdock::Component& component) {
    if (component.has_dpmeanlaplace()) return true;
    return false;
}

bool checkAllPathsPrivatized(const burdock::Analysis& analysis) {
    auto releaseNodes = getReleaseNodes(analysis);
    auto sourceNodes = getSources(analysis);

    return is_disjoint(releaseNodes, sourceNodes);
}

// adapted from https://stackoverflow.com/a/1964252/10221612
template<class Set1, class Set2>
bool is_disjoint(const Set1 &set1, const Set2 &set2) {
    if (set1.empty() || set2.empty()) return true;

    auto it1 = set1.begin(), it1End = set1.end();
    auto it2 = set2.begin(), it2End = set2.end();

    if (*it1 > *set2.rbegin() || *it2 > *set1.rbegin()) return true;

    while (it1 != it1End && it2 != it2End) {
        if (*it1 == *it2) return false;
        if (*it1 < *it2) it1++;
        else it2++;
    }

    return true;
}

DirectedGraph toGraph(const burdock::Analysis& analysis) {
    DirectedGraph graph;

    typedef boost::graph_traits<DirectedGraph>::vertex_descriptor Descriptor;

    // create vertices
    std::map<unsigned int, Descriptor> descriptors;
    for (const auto& nodePair : analysis.graph()) {
        Descriptor descriptor = graph.add_vertex(nodePair.second);
        descriptors[nodePair.first] = descriptor;
    }

    // create edges
    for (const auto& nodePair : analysis.graph()) {
        auto component = nodePair.second;
//        for (const auto& argumentPair : component.arguments())
//            graph.add_edge(descriptors[nodePair.first], descriptors[argumentPair.second.source_node_id()]);
    }

    return graph;
}