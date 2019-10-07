#ifndef OPENDP_LIBRARY_GRAPH_HPP
#define OPENDP_LIBRARY_GRAPH_HPP

#include <analysis.pb.h>
#include <boost/graph/directed_graph.hpp>

std::set<unsigned int> getSinks(const Analysis& analysis);
std::set<unsigned int> getSources(const Analysis& analysis);
std::set<unsigned int> getReleaseNodes (Analysis analysis);
bool isPrivatizer(const Component& component);

bool checkAllPathsPrivatized(const Analysis& analysis);
template<class Set1, class Set2>
bool is_disjoint(const Set1 &set1, const Set2 &set2);

typedef boost::directed_graph<Component> DirectedGraph;
DirectedGraph toGraph(const Analysis& analysis);

#endif //OPENDP_LIBRARY_GRAPH_HPP
