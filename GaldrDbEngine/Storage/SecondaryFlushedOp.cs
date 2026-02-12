namespace GaldrDbEngine.Storage;

internal struct SecondaryFlushedOp
{
    public LeafOpType Type;
    public byte[] Key;
    public DocumentLocation OldLocation; // for undo of delete/update
}
