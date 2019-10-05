#include "../include/differential_privacy/graph.hpp"

#include "analysis.pb.h"
#include <iostream>
#include <queue>

// Uncomment to force error if protobuf versions mismatch
//GOOGLE_PROTOBUF_VERIFY_VERSION;

unsigned int validateAnalysis(char* analysisBuffer, size_t analysisLength) {

    std::string analysisString(analysisBuffer, analysisLength);
    Analysis analysis;
    analysis.ParseFromString(analysisString);

    bool validity = true;
    if (!checkAllPathsPrivatized(analysis)) validity = false;

    // check that this function works
    toGraph(analysis);

    google::protobuf::ShutdownProtobufLibrary();
    return validity;
}

double computeEpsilon(char* analysisBuffer, size_t analysisLength) {

    std::string analysisString(analysisBuffer, analysisLength);
    Analysis analysis;
    analysis.ParseFromString(analysisString);

    // TODO: compute epsilon
    return 23.2;
}

char* generateReport(
        char* analysisBuffer, size_t analysisLength,
        char* releaseBuffer, size_t releaseLength) {

    std::string reportString(R"({"message": "this is a release in the json schema format"})");
    return &reportString[0];
}

std::set<unsigned int> getSinks(const Analysis& analysis) {
    auto nodeIds = std::set<unsigned int>();
    for (const auto& nodePair : analysis.graph())
        nodeIds.insert(nodePair.first);

    for (const auto& nodePair : analysis.graph())
        for (const auto& argumentPair : nodePair.second.arguments())
            nodeIds.erase(argumentPair.first);

    return nodeIds;
}

std::set<unsigned int> getSources(const Analysis& analysis) {
    auto nodeIds = std::set<unsigned int>();
    for (const auto& nodePair : analysis.graph()) {
        if (nodePair.second.arguments_size() > 0) continue;
        nodeIds.insert(nodePair.first);
    }
    return nodeIds;
}

std::set<unsigned int> getReleaseNodes(Analysis analysis) {

    std::set<unsigned int> releaseNodeIds;
    auto sinkIds = getSinks(analysis);
    std::queue<unsigned int, std::deque<unsigned int>> nodeQueue(
            std::deque<unsigned int>(sinkIds.begin(), sinkIds.end()));

    auto graph = *analysis.mutable_graph();
    while (!nodeQueue.empty()) {
        unsigned int nodeId = nodeQueue.front();
        nodeQueue.pop();

        Component component = graph[nodeId];
        if (isPrivatizer(component))
            releaseNodeIds.insert(nodeId);
        else
            for (const auto& argumentPair : component.arguments())
                nodeQueue.push(argumentPair.first);
    }

    return releaseNodeIds;
}

bool isPrivatizer(const Component& component) {
    if (component.has_mean()) return true;
    return false;
}

bool checkAllPathsPrivatized(const Analysis& analysis) {
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

DirectedGraph toGraph(const Analysis& analysis) {
    DirectedGraph graph;

    typedef boost::graph_traits<DirectedGraph>::vertex_descriptor Descriptor;

    std::map<unsigned int, Descriptor> descriptors;
    for (const auto& nodePair : analysis.graph()) {
        Descriptor descriptor = graph.add_vertex(nodePair.second);
        descriptors[nodePair.first] = descriptor;
    }

    for (const auto& nodePair : analysis.graph()) {
        auto component = nodePair.second;
        for (const auto& argumentPair : component.arguments())
            graph.add_edge(descriptors[nodePair.first], descriptors[argumentPair.first]);
    }

    return graph;
}