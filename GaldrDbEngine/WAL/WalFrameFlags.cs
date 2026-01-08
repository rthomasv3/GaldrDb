using System;

namespace GaldrDbEngine.WAL;

[Flags]
public enum WalFrameFlags : byte
{
    None = 0x00,
    Commit = 0x01,
    Checkpoint = 0x02
}
