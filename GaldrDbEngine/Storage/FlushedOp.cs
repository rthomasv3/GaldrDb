namespace GaldrDbEngine.Storage;

internal struct FlushedOp
{
    public LeafOpType Type;
    public int Key;
    public DocumentLocation OldLocation; // for undo of delete/update
}
