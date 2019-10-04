#include "../include/differential_privacy/graph.hpp"

#include "analysis.pb.h"
#include <iostream>
#include <queue>

// Uncomment to force error if protobuf versions mismatch
//GOOGLE_PROTOBUF_VERIFY_VERSION;

signed int validateAnalysis(char* analysisBuffer, size_t length) {

    std::string analysisString(analysisBuffer, length);
    Analysis analysis;
    analysis.ParseFromString(analysisString);

    bool validity = true;
    if (!checkAllPathsPrivatized(analysis)) validity = false;

    google::protobuf::ShutdownProtobufLibrary();
    return validity;
}

std::set<unsigned int> getSinks(const Analysis& analysis) {
    auto nodeIds = std::set<unsigned int>();
    for (const std::pair<unsigned int, Component>& nodePair : analysis.graph())
        nodeIds.insert(nodePair.first);

    for (const std::pair<unsigned int, Component>& node : analysis.graph())
        for (const std::pair<std::string, unsigned int>& argument : node.second.arguments())
            nodeIds.erase(argument.second);

    return nodeIds;
}

std::set<unsigned int> getSources(const Analysis& analysis) {
    auto nodeIds = std::set<unsigned int>();
    for (const std::pair<unsigned int, Component>& nodePair : analysis.graph()) {
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
                nodeQueue.push(argumentPair.second);
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