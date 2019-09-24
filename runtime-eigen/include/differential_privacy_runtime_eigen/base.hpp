
#ifndef DIFFERENTIAL_PRIVACY_BASE_HPP
#define DIFFERENTIAL_PRIVACY_BASE_HPP

#include <map>
#include <vector>
#include "../../../base/include/differential_privacy/components.hpp"

template <class T>
class Column {
    std::vector<T>* _data = nullptr;
public:
    explicit Column(std::vector<T>* data) {
        _data = data;
    }
    std::vector<T>* get_data() {
        return _data;
    }
};

template<class T>
class DataFrame {
    std::map<std::string, Column<T>>* _data;
public:
    explicit DataFrame(std::map<std::string, Column<T>>* data) {
        _data = data;
    }
};

typedef std::map<std::string, DataFrame<double>> DataSet;

template <class T>
class Release {
    T* _value = nullptr;
    std::string _id;
public:
    Release(T* value, std::string id) {
        _value = value;
        _id = id;
    }
};

void release(Component* component, std::vector<double> data);


#endif //DIFFERENTIAL_PRIVACY_BASE_HPP
