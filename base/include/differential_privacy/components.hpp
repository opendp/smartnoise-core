#ifndef DIFFERENTIAL_PRIVACY_COMPONENTS_HPP
#define DIFFERENTIAL_PRIVACY_COMPONENTS_HPP

#include <list>
#include <string>
#include <utility>
#include "privacy_definition.hpp"

typedef std::pair<std::string, std::string> DatasourceTag;

// most elementary primitive
class Component {
protected:
    bool _will_release = false;

    double _epsilon = std::numeric_limits<double>::quiet_NaN();
    std::list<Component*> _children = std::list<Component*>();

public:
    explicit Component();
    explicit Component(Component* child);
    explicit Component(std::list<Component*> children);

    virtual std::string get_name();
    virtual std::list<DatasourceTag>* get_sources();

    double get_epsilon();
    std::list<Component*> get_children();

    bool set_will_release(bool state);
    bool get_will_release();
};

// components that obfuscate data
class Mechanism : public Component {
    PrivacyDefinition _privacy_definition;
public:
    explicit Mechanism(Component* child);
};

// components that transform data
class Transform : public Component {
public:
    explicit Transform(Component* child);
};

class Aggregate : public Component {
public:
    explicit Aggregate(Component* child);
};

// parallel connected components
class Analysis : public Component {
protected:
    std::string _name = "analysis";
public:
    explicit Analysis() : Component() {};
    explicit Analysis(Component* child) : Component(child) {};
    explicit Analysis(std::list<Component*> children) : Component(std::move(children)) {};
    bool add(Component* child);
};

// component that identifies which data input to use
class Datasource : public Component {
    std::string _dataset;
    std::string _column;
protected:
    std::string _name = "datasource";
public:
    explicit Datasource(std::string dataset, std::string column);
    std::list<DatasourceTag>* get_sources() override;
};

class CountVectorize : public Component {
protected:
    std::string _name = "count-vectorize";
public:
    explicit CountVectorize(Component child);
};

#endif //DIFFERENTIAL_PRIVACY_COMPONENTS_HPP
