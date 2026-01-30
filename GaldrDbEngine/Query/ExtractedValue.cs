namespace GaldrDbEngine.Query;

internal readonly struct ExtractedValue
{
    public object Value { get; }

    public ExtractedValue(object value)
    {
        Value = value;
    }
}
