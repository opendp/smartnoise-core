#include "../include/differential_privacy/base.hpp"
#include <Eigen/Dense>
#include <iostream>
#include <cmath>
#include <utility>


Component::Component() {
    this->_children = new std::list<Component>({});
}
Component::Component(Component* child) {
    this->_children = new std::list<Component>({*child});
}
Component::Component(std::list<Component>* children) {
    this->_children = children;
}
bool Component::set_will_release(bool state) {
    this->_will_release = state;
    return true;
}
bool Component::get_will_release() {
    return this->_will_release;
}
std::string Component::get_name() {
    return this->_name;
}
double Component::get_epsilon() {
    if (!std::isnan(this->_epsilon)) return this->_epsilon;
    double total = 0;
    if (this->_children != nullptr) {
        for (auto child : *this->_children) {
            double child_epsilon = child.get_epsilon();
            total += std::isnan(child_epsilon) ? 0 : child_epsilon;
        }
    }
    return total;
}

Aggregate::Aggregate(Component child) : Component(&child) {}
Transform::Transform(Component child) : Component(&child) {}
Mechanism::Mechanism(Component child) : Component(&child) {}
Datasource::Datasource(std::string tag) : Component(), tag{std::move(tag)} {}

bool Analysis::add(const Component& child) {
    this->_children->push_back(child);
}