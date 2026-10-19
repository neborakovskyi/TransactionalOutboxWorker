CREATE PROCEDURE [dbo].[pub_ClaimBatch_Get]
	@BatchSize INT,
	@WorkerId INT,
	@LeaseSeconds INT
AS
BEGIN TRANSACTION;
BEGIN TRY
    ;WITH [Candidates] AS
    (
        -- TOP + ORDER BY живут в CTE, потому что в самом UPDATE ORDER BY недоступен.
        SELECT TOP (@BatchSize) 
            [Id]              
            ,[EventId]         
            ,[Type]            
            ,[Payload]         
            ,[CreatedAtUtc]    
            ,[PublishedAtUtc]  
            ,[NextAttemptAtUtc]
            ,[LockedUntilUtc]  
            ,[LockOwner]       
            ,[Attempts]        
            ,[Status]          
            ,[LastError]       
        FROM [dbo].[OutboxMessages]
                WITH (UPDLOCK,   -- сразу update-lock: без shared→update конверсии и deadlock'ов
                    READPAST,  -- строки, залоченные другими Worker, пропускаем, а не ждём
                    ROWLOCK)   -- не даём эскалироваться до блокировки таблицы
        WHERE
            (
                -- Случай 1: новая запись или запись после backoff, готовая к повтору
                [Status] = N'Pending'
                AND [NextAttemptAtUtc] <= SYSUTCDATETIME()
            )
            OR
            (
                -- Случай 2: прежний Worker упал или завис, аренда истекла
                [Status] = N'Processing'
                AND [LockedUntilUtc] <= SYSUTCDATETIME()
            )
        ORDER BY [Id]  -- приблизительный FIFO (строгий порядок не гарантируется)
    )
    UPDATE [Candidates]
    SET [Status]       = N'Processing',
        [LockOwner]      = @WorkerId,
        -- аренда считается по часам SQL Server, а не приложения (нет clock skew между инстансами)
        [LockedUntilUtc] = DATEADD(SECOND, @LeaseSeconds, SYSUTCDATETIME()),
        -- Attempts растёт при захвате: poison-запись, убивающая процесс, тоже исчерпает лимит
        [Attempts]       = [Attempts] + 1
    -- OUTPUT возвращает ровно те строки, которые мы только что захватили,
    -- в том же round-trip: отдельного SELECT не нужно.
    OUTPUT inserted.[Id] 
           ,inserted.[EventId] 
           ,inserted.[Type] 
           ,inserted.[Payload]
           ,inserted.[CreatedAtUtc] 
           ,inserted.[PublishedAtUtc] 
           ,inserted.[NextAttemptAtUtc]
           ,inserted.[LockedUntilUtc] 
           ,inserted.[LockOwner] 
           ,inserted.[Attempts]
           ,inserted.[Status] 
           ,inserted.[LastError];
    COMMIT TRANSACTION;
END TRY
BEGIN CATCH
    IF XACT_STATE() <> 0
        ROLLBACK TRANSACTION;
    THROW;
END CATCH;