#ifndef OPENDP_LIBRARY_GRAPH_HPP
#define OPENDP_LIBRARY_GRAPH_HPP

#include <analysis.pb.h>
#include <boost/graph/directed_graph.hpp>

std::set<unsigned int> getSinks(const burdock::Analysis& analysis);
std::set<unsigned int> getSources(const burdock::Analysis& analysis);
std::set<unsigned int> getReleaseNodes (burdock::Analysis analysis);
bool isPrivatizer(const burdock::Component& component);

bool checkAllPathsPrivatized(const burdock::Analysis& analysis);
template<class Set1, class Set2>
bool is_disjoint(const Set1 &set1, const Set2 &set2);

typedef boost::directed_graph<burdock::Component> DirectedGraph;
DirectedGraph toGraph(const burdock::Analysis& analysis);

#endif //OPENDP_LIBRARY_GRAPH_HPP
