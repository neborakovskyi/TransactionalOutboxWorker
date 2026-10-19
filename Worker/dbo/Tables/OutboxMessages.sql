CREATE TABLE [dbo].[OutboxMessages]
(
	[Id]               BIGINT IDENTITY(1,1) NOT NULL CONSTRAINT PK_OutboxMessages PRIMARY KEY,
    [EventId]          UNIQUEIDENTIFIER     NOT NULL,
    [Type]             NVARCHAR(256)        NOT NULL,
    [Payload]          NVARCHAR(MAX)        NOT NULL,
    [CreatedAtUtc]     DATETIME2(3)         NOT NULL CONSTRAINT DF_Outbox_Created DEFAULT SYSUTCDATETIME(),
    [PublishedAtUtc]   DATETIME2(3)         NULL,
    [NextAttemptAtUtc] DATETIME2(3)         NOT NULL CONSTRAINT DF_Outbox_Next DEFAULT SYSUTCDATETIME(),
    [LockedUntilUtc]   DATETIME2(3)         NULL,
    [LockOwner]        NVARCHAR(128)        NULL,
    [Attempts]         INT                  NOT NULL CONSTRAINT DF_Outbox_Attempts DEFAULT 0,
    [Status]           NVARCHAR(16)         NOT NULL CONSTRAINT DF_Outbox_Status DEFAULT N'Pending',
    [LastError]        NVARCHAR(2000)       NULL,
    CONSTRAINT UQ_OutboxMessages_EventId UNIQUE ([EventId])
);

GO

CREATE INDEX IX_OutboxMessages_Active ON [dbo].[OutboxMessages]([Id])
    INCLUDE ([NextAttemptAtUtc], [LockedUntilUtc])
    WHERE [Status] IN (N'Pending', N'Processing');
