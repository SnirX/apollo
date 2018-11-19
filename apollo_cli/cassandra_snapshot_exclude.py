"""
Exclude Keyspaces and tables from snapshot since Cassandra 2.0.x doesn't snapshot all keyspaces/snapshots
"""

keyspaces_exclude = {'system_traces'}
tables_exclude = {
    'system': {'schema_triggers', 'compaction_history', 'hints', 'paxos', 'peer_events', 'range_xfers', 'schema_triggers', 'batchlog'}
}
