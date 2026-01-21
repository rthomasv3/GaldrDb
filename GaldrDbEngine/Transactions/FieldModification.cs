using GaldrDbEngine.Query;

namespace GaldrDbEngine.Transactions;

/// <summary>
/// Represents a pending field modification for a partial update.
/// </summary>
internal struct FieldModification
{
    public string FieldName;
    public GaldrFieldType FieldType;
    public object Value;
}
