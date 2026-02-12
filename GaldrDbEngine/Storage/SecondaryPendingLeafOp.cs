namespace GaldrDbEngine.Storage;

internal struct SecondaryPendingLeafOp
{
    public ulong TxId;
    public LeafOpType OpType;
    public byte[] Key;
    public DocumentLocation Location;     // value for Insert/Update
    public DocumentLocation OldLocation;  // original value for Delete/Update (for undo)
}
