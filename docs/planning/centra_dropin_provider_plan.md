# Flotilla Centra Drop-in Provider Specification

## Architecture Overview

This plan defines how Flotilla functions as a drop-in transport provider for **Centra PubSub** (`Centra.Providers.Flotilla` and `Centra.Providers.Flotilla.Hosting`), replacing RabbitMQ across the distributed application ecosystem.

```
+-------------------------------------------------------------+
|             Centra PubSub Application Layer                 |
|                                                             |
|  [ IntegrationAdtConsumer ]     [ IntegrationSiuConsumer ]  |
|           \                                 /               |
|            +---------------+---------------+                |
|                            |                                |
|                 [ Centra PubSub Router ]                    |
|                            |                                |
|        [ Centra.Providers.Flotilla.PubSubDriver ]           |
+----------------------------|--------------------------------+
                             |
                   UDP Raft Protocol (< 80µs)
                             |
                             v
+-------------------------------------------------------------+
|               Flotilla Consensus Cluster                    |
|                                                             |
|  [ Node 1 (Leader) ] <---> [ Node 2 ] <---> [ Node 3 ]      |
|           │                                                 |
|     Committed Log                                           |
|           │                                                 |
|           v                                                 |
|  [ CosmosArchiveSink ] ──> Azure Cosmos DB ingress-journal-v2
+-------------------------------------------------------------+
```

### Key Integration Points:
1. **Wire Encoding**: Flotilla messages carry topic, headers, and binary payload in a compact framed layout (`FlotillaWireProtocol`).
2. **Proposals**: Inbound messages are proposed to Flotilla's leader via UDP framing with CRC32 checksums (< 80µs quorum commitment).
3. **Commit Stream**: The Centra driver tails committed entries sequentially and invokes `IEventHandler<T>.HandleAsync`.
4. **Zero Consumer Impact**: No changes to domain actors or Centra event handlers.
5. **Decoupled Cosmos Archival**: Flotilla's `CosmosArchiveSink` directly archives committed entries to `ingress-journal-v2`, completely freeing the .NET pipeline from journal writes.

Refer to the primary implementation plan in `distributed-framework`:
[`docs/plans/flotilla-provider-plan.md`](file:///home/chad/source/dotnet/distributed-framework/docs/plans/flotilla-provider-plan.md) and [`docs/providers/flotilla.md`](file:///home/chad/source/dotnet/distributed-framework/docs/providers/flotilla.md).
