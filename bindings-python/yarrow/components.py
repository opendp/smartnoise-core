from . import Component


def covariance(left, right, **kwargs):
    """covariance step"""
    return Component(
        "Covariance",
        arguments={
            'left': Component.of(left),
            'right': Component.of(right)
        },
        options={
            
        },
        constraints=kwargs)


def dp_median(data, privacy_usage, **kwargs):
    """dp_median step"""
    return Component(
        "DPMedian",
        arguments={
            'data': Component.of(data)
        },
        options={
            'privacy_usage': privacy_usage
        },
        constraints=kwargs)


def modulus(left, right, **kwargs):
    """modulus step"""
    return Component(
        "Modulus",
        arguments={
            'left': Component.of(left),
            'right': Component.of(right)
        },
        options={
            
        },
        constraints=kwargs)


def logical_or(left, right, **kwargs):
    """logical_or step"""
    return Component(
        "Or",
        arguments={
            'left': Component.of(left),
            'right': Component.of(right)
        },
        options={
            
        },
        constraints=kwargs)


def resize(data, n, **kwargs):
    """resize step"""
    return Component(
        "Resize",
        arguments={
            'data': Component.of(data),
            'n': Component.of(n)
        },
        options={
            
        },
        constraints=kwargs)


def variance(data, **kwargs):
    """variance step"""
    return Component(
        "Variance",
        arguments={
            'data': Component.of(data)
        },
        options={
            
        },
        constraints=kwargs)


def literal(value, private, **kwargs):
    """pass forward the data contained within the protobuf as an array"""
    return Component(
        "Literal",
        arguments={
            
        },
        options={
            'value': value,
            'private': private
        },
        constraints=kwargs)


def materialize(dataset_id, **kwargs):
    """materialize step"""
    return Component(
        "Materialize",
        arguments={
            
        },
        options={
            'dataset_id': dataset_id
        },
        constraints=kwargs)


def add(left, right, **kwargs):
    """add step"""
    return Component(
        "Add",
        arguments={
            'left': Component.of(left),
            'right': Component.of(right)
        },
        options={
            
        },
        constraints=kwargs)


def bin(data, edges, interval, **kwargs):
    """bin step"""
    return Component(
        "Bin",
        arguments={
            'data': Component.of(data),
            'edges': Component.of(edges)
        },
        options={
            'interval': interval
        },
        constraints=kwargs)


def multiply(left, right, **kwargs):
    """multiply step"""
    return Component(
        "Multiply",
        arguments={
            'left': Component.of(left),
            'right': Component.of(right)
        },
        options={
            
        },
        constraints=kwargs)


def moment_raw(data, order, **kwargs):
    """moment_raw step"""
    return Component(
        "MomentRaw",
        arguments={
            'data': Component.of(data)
        },
        options={
            'order': order
        },
        constraints=kwargs)


def less_than(left, right, **kwargs):
    """less_than step"""
    return Component(
        "LessThan",
        arguments={
            'left': Component.of(left),
            'right': Component.of(right)
        },
        options={
            
        },
        constraints=kwargs)


def mean(data, **kwargs):
    """mean step"""
    return Component(
        "Mean",
        arguments={
            'data': Component.of(data)
        },
        options={
            
        },
        constraints=kwargs)


def dp_sum(data, group_by, labels, privacy_usage, **kwargs):
    """dp_sum step"""
    return Component(
        "DPSum",
        arguments={
            'data': Component.of(data),
            'group_by': Component.of(group_by)
        },
        options={
            'labels': labels,
            'privacy_usage': privacy_usage
        },
        constraints=kwargs)


def power(data, radical, **kwargs):
    """power step"""
    return Component(
        "Power",
        arguments={
            'data': Component.of(data),
            'radical': Component.of(radical)
        },
        options={
            
        },
        constraints=kwargs)


def cast(data, type, true_label=None, min=None, max=None, **kwargs):
    """cast step"""
    return Component(
        "Cast",
        arguments={
            'data': Component.of(data),
            'type': Component.of(type),
            'true_label': Component.of(true_label),
            'min': Component.of(min),
            'max': Component.of(max)
        },
        options={
            
        },
        constraints=kwargs)


def impute(data, distribution, data_type, min, max, shift, scale, **kwargs):
    """impute step"""
    return Component(
        "Impute",
        arguments={
            'data': Component.of(data),
            'distribution': Component.of(distribution),
            'data_type': Component.of(data_type),
            'min': Component.of(min),
            'max': Component.of(max),
            'shift': Component.of(shift),
            'scale': Component.of(scale)
        },
        options={
            
        },
        constraints=kwargs)


def divide(left, right, **kwargs):
    """divide step"""
    return Component(
        "Divide",
        arguments={
            'left': Component.of(left),
            'right': Component.of(right)
        },
        options={
            
        },
        constraints=kwargs)


def dp_covariance(data_left, data_right, privacy_usage, **kwargs):
    """dp_covariance step"""
    return Component(
        "DPCovariance",
        arguments={
            'data_left': Component.of(data_left),
            'data_right': Component.of(data_right)
        },
        options={
            'privacy_usage': privacy_usage
        },
        constraints=kwargs)


def dp_mean(data, privacy_usage, **kwargs):
    """dp_mean step"""
    return Component(
        "DPMean",
        arguments={
            'data': Component.of(data)
        },
        options={
            'privacy_usage': privacy_usage
        },
        constraints=kwargs)


def dp_variance(data, privacy_usage, **kwargs):
    """dp_variance step"""
    return Component(
        "DPVariance",
        arguments={
            'data': Component.of(data)
        },
        options={
            'privacy_usage': privacy_usage
        },
        constraints=kwargs)


def median(data, **kwargs):
    """median step"""
    return Component(
        "Median",
        arguments={
            'data': Component.of(data)
        },
        options={
            
        },
        constraints=kwargs)


def log(base, data, **kwargs):
    """log step"""
    return Component(
        "Log",
        arguments={
            'base': Component.of(base),
            'data': Component.of(data)
        },
        options={
            
        },
        constraints=kwargs)


def dp_count(data, group_by, labels, privacy_usage, **kwargs):
    """dp_count step"""
    return Component(
        "DPCount",
        arguments={
            'data': Component.of(data),
            'group_by': Component.of(group_by)
        },
        options={
            'labels': labels,
            'privacy_usage': privacy_usage
        },
        constraints=kwargs)


def equal(left, right, **kwargs):
    """equal step"""
    return Component(
        "Equal",
        arguments={
            'left': Component.of(left),
            'right': Component.of(right)
        },
        options={
            
        },
        constraints=kwargs)


def to_string(data, **kwargs):
    """to_string step"""
    return Component(
        "ToString",
        arguments={
            'data': Component.of(data)
        },
        options={
            
        },
        constraints=kwargs)


def div(left, right, **kwargs):
    """div step"""
    return Component(
        "Div",
        arguments={
            'left': Component.of(left),
            'right': Component.of(right)
        },
        options={
            
        },
        constraints=kwargs)


def negate(data, **kwargs):
    """negate step"""
    return Component(
        "Negate",
        arguments={
            'data': Component.of(data)
        },
        options={
            
        },
        constraints=kwargs)


def subtract(left, right, **kwargs):
    """subtract step"""
    return Component(
        "Subtract",
        arguments={
            'left': Component.of(left),
            'right': Component.of(right)
        },
        options={
            
        },
        constraints=kwargs)


def index(data, path, **kwargs):
    """index step"""
    return Component(
        "Index",
        arguments={
            'data': Component.of(data)
        },
        options={
            'path': path
        },
        constraints=kwargs)


def row_wise_min(left, right, **kwargs):
    """row_wise_min step"""
    return Component(
        "RowMin",
        arguments={
            'left': Component.of(left),
            'right': Component.of(right)
        },
        options={
            
        },
        constraints=kwargs)


def sum(data, by, categories, **kwargs):
    """sum step"""
    return Component(
        "Sum",
        arguments={
            'data': Component.of(data),
            'by': Component.of(by),
            'categories': Component.of(categories)
        },
        options={
            
        },
        constraints=kwargs)


def clamp(data, min, max, categories, null_value, **kwargs):
    """clamp step"""
    return Component(
        "Clamp",
        arguments={
            'data': Component.of(data),
            'min': Component.of(min),
            'max': Component.of(max),
            'categories': Component.of(categories),
            'null_value': Component.of(null_value)
        },
        options={
            
        },
        constraints=kwargs)


def row_wise_max(left, right, **kwargs):
    """row_wise_max step"""
    return Component(
        "RowMax",
        arguments={
            'left': Component.of(left),
            'right': Component.of(right)
        },
        options={
            
        },
        constraints=kwargs)


def logical_and(left, right, **kwargs):
    """logical_and step"""
    return Component(
        "And",
        arguments={
            'left': Component.of(left),
            'right': Component.of(right)
        },
        options={
            
        },
        constraints=kwargs)


def constant(**kwargs):
    """intended to be used with a release that already contains the value for this node"""
    return Component(
        "Constant",
        arguments={
            
        },
        options={
            
        },
        constraints=kwargs)


def greater_than(left, right, **kwargs):
    """greater_than step"""
    return Component(
        "GreaterThan",
        arguments={
            'left': Component.of(left),
            'right': Component.of(right)
        },
        options={
            
        },
        constraints=kwargs)


def count(categories, data, **kwargs):
    """count step"""
    return Component(
        "Count",
        arguments={
            'categories': Component.of(categories),
            'data': Component.of(data)
        },
        options={
            'categories': categories
        },
        constraints=kwargs)


def to_int(data, **kwargs):
    """to_int step"""
    return Component(
        "ToInt",
        arguments={
            'data': Component.of(data)
        },
        options={
            
        },
        constraints=kwargs)


def dp_moment_raw(data, order, privacy_usage, **kwargs):
    """dp_moment_raw step"""
    return Component(
        "DPMomentRaw",
        arguments={
            'data': Component.of(data)
        },
        options={
            'order': order,
            'privacy_usage': privacy_usage
        },
        constraints=kwargs)


def stack(**kwargs):
    """stack step"""
    return Component(
        "Stack",
        arguments={**kwargs, **{
            
        }},
        options={
            
        },
        constraints=None)

