import React from 'react';
import {Table} from "@awsui/components-react";


export default function SpectrumDiff() {
    const COL_DEF = [
        {
            id: 'id',
            header: 'Replay',
            cell: item => item.id,
            width: 50
        },
        {
            id: 'cluster',
            header: 'Cluster',
            cell: item => item.cluster,
            width: 50
        },
        {
            id: 'instance',
            header: 'Instance',
            cell: item => item.instance,
            width: 50
        },
        {
            id: 'nodes',
            header: 'Nodes',
            cell: item => item.nodes,
            width: 50
        },
        {
            id: 'db',
            header: 'Database',
            cell: item => item.db,
            width: 50
        },
        {
            id: 'start',
            header: 'Start Time',
            cell: item => item.start,
            width: 50
        },
        {
            id: 'end',
            header: 'End Time',
            cell: item => item.end,
            width: 50
        },
        {
            id: 'duration',
            header: 'Duration',
            cell: item => item.duration,
            width: 50
        },
        {
            id: 'executed',
            header: 'Queries Executed',
            cell: item => item.executed,
            width: 50
        },
        {
            id: 'aborted',
            header: 'Queries Aborted',
            cell: item => item.aborted,
            width: 50
        },
        {
            id: 'connections',
            header: 'Connections',
            cell: item => item.connections,
            width: 50
        },


    ]

    return (
        <Table items={[]} columnDefinitions={COL_DEF}></Table>
    )
};