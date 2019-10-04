#ifndef OPENDP_LIBRARY_GRAPH_HPP
#define OPENDP_LIBRARY_GRAPH_HPP

#include <cstddef>
#include <analysis.pb.h>

extern "C" {
    signed int validateAnalysis(char* analysisBuffer, size_t length);
}

std::set<unsigned int> getSinks(const Analysis& analysis);
std::set<unsigned int> getSources(const Analysis& analysis);
std::set<unsigned int> getReleaseNodes (Analysis analysis);
bool isPrivatizer(const Component& component);

bool checkAllPathsPrivatized(const Analysis& analysis);
template<class Set1, class Set2>
bool is_disjoint(const Set1 &set1, const Set2 &set2);

#endif //OPENDP_LIBRARY_GRAPH_HPP
