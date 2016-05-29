Data format:

Table Header:

    Magic           : long
    Format major    : short
    Format minor    : short
    MinGeneration   : long
    MaxGeneration   : long
    Header checksum : long

    Completion status: will be 0 initially, but updated when writing concludes

    End of data     : long
    Compl. checksum : long

Batch:

    (Tombstone | Put)* Start-Data Data* Commit

    with:

    Tombstone:

        Tag             : byte
        Key length      : short
        Key             : byte[]

    Payload:

        Tag             : byte
        Key length      : short
        Payload length  : int
        Key             : byte[]

    Start-Data:

        Tag             : byte

    Commit:

        Tag             : byte
        Checksum        : long
