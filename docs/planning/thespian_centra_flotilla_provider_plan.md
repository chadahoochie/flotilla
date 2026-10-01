# Flotilla Centra Drop-in Provider Specification

## Architecture Overview

This plan defines how Flotilla functions as a drop-in transport provider for **Centra PubSub** in `VaxCare.ThespianApi`, replacing RabbitMQ.

```
+-------------------------------------------------------------+
|             VaxCare.Thespian.Integration.Api                |
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
1. **Wire Encoding**: Flotilla messages carry topic, headers, and binary payload in a compact framed layout.
2. **Proposals**: Inbound EHR messages are proposed to Flotilla's leader via UDP framing with CRC32 checksums.
3. **Commit Stream**: The Centra driver tails committed entries sequentially and invokes `IEventHandler<T>.HandleAsync`.
4. **Zero Consumer Impact**: No changes to Thespian domain actors or Centra event handlers.
5. **Decoupled Cosmos Archival**: Flotilla's `CosmosArchiveSink` directly archives committed entries to `ingress-journal-v2`, completely freeing the .NET pipeline from journal writes.

Refer to the primary integration document in `VaxCare.ThespianApi`:
[`docs/planning/flotilla-centra-dropin-replacement-plan.md`](file:///home/chad/source/dotnet/VaxCare.ThespianApi/docs/planning/flotilla-centra-dropin-replacement-plan.md).
