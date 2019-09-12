#include "../include/differential_privacy/components.hpp"
#include <cmath>
#include <utility>


Component::Component() {
    this->_children = std::list<Component*>({});
}
Component::Component(Component* child) {
    this->_children = std::list<Component*>({child});
}
Component::Component(std::list<Component*> children) {
    this->_children = std::move(children);
}
bool Component::set_will_release(bool state) {
    this->_will_release = state;
    return true;
}
bool Component::get_will_release() {
    return this->_will_release;
}
std::string Component::get_name() {
    return "component";
}

std::list<DatasourceTag>* Component::get_sources() {
    auto* sources = new std::list<std::pair<std::string, std::string>>();
    for (Component* child : this->_children)
        for (const auto& source : *child->get_sources())
            sources->push_back(source);
    return sources;
}

double Component::get_epsilon() {
    if (!std::isnan(this->_epsilon)) return this->_epsilon;
    double total = 0;
    for (auto* child : this->_children) {
        double child_epsilon = child->get_epsilon();
        total += std::isnan(child_epsilon) ? 0 : child_epsilon;
    }
    return total;
}

std::list<Component*> Component::get_children() {
    return this->_children;
}
Aggregate::Aggregate(Component* child) : Component(child) {}
Transform::Transform(Component* child) : Component(child) {}
Mechanism::Mechanism(Component* child) : Component(child) {}

Datasource::Datasource(std::string dataset, std::string column) : Component(), _dataset{std::move(dataset)}, _column{std::move(column)} {}
std::list<DatasourceTag>* Datasource::get_sources() {
    return new std::list<DatasourceTag>({{this->_dataset, this->_column}});
}

CountVectorize::CountVectorize(Component child) : Component(&child) {};

bool Analysis::add(Component* child) {
    this->_children.push_back(child);
    return false;
}
