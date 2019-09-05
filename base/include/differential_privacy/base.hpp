#ifndef DIFFERENTIAL_PRIVACY_BASE_HPP
#define DIFFERENTIAL_PRIVACY_BASE_HPP

#include <list>
#include <string>
#include "privacy_definition.hpp"
#include "iostream"

// most elementary primitive
class Component {
protected:
    std::string _name;
    bool _will_release = false;
    bool _privatizer = false;

    double _epsilon = std::numeric_limits<double>::quiet_NaN();
    std::list<Component>* _children = nullptr;

public:
    explicit Component();
    explicit Component(Component* child);
    explicit Component(std::list<Component>* children);

    virtual std::string get_name();

    double get_epsilon();

    bool set_will_release(bool state);
    bool get_will_release();
};

// components that obfuscate data
class Mechanism : public Component {
    bool _privatizer = true;
public:
    explicit Mechanism(Component child);
};

// components that transform data
class Transform : public Component {
public:
    explicit Transform(Component child);
};

class Aggregate : public Component {
public:
    explicit Aggregate(Component child);
};

// parallel connected components
class Analysis : public Component {
public:
    explicit Analysis() : Component() {};
    explicit Analysis(Component child) : Component(&child) {};
    explicit Analysis(std::list<Component>* children) : Component(children) {};
    bool add(const Component& child);
    PrivacyDefinition privacy_definition;
};

// component that identifies which data input to use
class Datasource : public Component {
    std::string tag;
public:
    explicit Datasource(std::string tag);
};

#endif //DIFFERENTIAL_PRIVACY_BASE_HPP