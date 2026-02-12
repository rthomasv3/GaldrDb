namespace GaldrDbEngine.Storage;

internal struct PendingLeafOp
{
    public ulong TxId;
    public LeafOpType OpType;
    public int Key;
    public DocumentLocation Location;     // value for Insert/Update
    public DocumentLocation OldLocation;  // original value for Delete/Update (for undo)
}
