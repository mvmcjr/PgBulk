using System.Reflection;
using Microsoft.EntityFrameworkCore.Storage.ValueConversion;
using PgBulk.Abstractions;

namespace PgBulk.EFCore;

public record EntityColumnInformation : ManualTableColumnMapping, ITableColumnInformation
{
    private readonly ValueConverter? _valueConverter;

    public EntityColumnInformation(string name, bool primaryKey, bool valueGeneratedOnAdd, PropertyInfo? property, int index, ValueConverter? valueConverter = null) : base(name, property, valueGeneratedOnAdd, index, primaryKey)
    {
        Property = property;
        _valueConverter = valueConverter;
    }

    public PropertyInfo? Property { get; }

    object? ITableColumnInformation.GetValue(object entity)
    {
        var value = base.GetValue(entity);
        if (_valueConverter != null && value != null)
            value = _valueConverter.ConvertToProvider(value);
        return value;
    }
}