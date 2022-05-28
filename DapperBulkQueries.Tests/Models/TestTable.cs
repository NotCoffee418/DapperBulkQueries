using System.Diagnostics.CodeAnalysis;

public class TestTable
{
    public ulong Id { get; set; }
    public string TextCol { get; set; }
    public decimal NumberCol { get; set; }
    public bool BoolCol { get; set; }

    /// <summary>
    /// True when all properties, excluding ID, are the same.
    /// </summary>
    /// <param name="other"></param>
    /// <returns></returns>
    public bool IsPropertiesMatch(TestTable? other)
        => other is not null &&
        TextCol == other.TextCol &&
        NumberCol == other.NumberCol &&
        BoolCol == other.BoolCol;

    /// <summary>
    /// True when the entire object is equal
    /// </summary>
    /// <param name="other"></param>
    /// <returns></returns>
    public bool IsFullMatch(TestTable? other)
        => IsPropertiesMatch(other) &&
        Id == other?.Id;
}

    