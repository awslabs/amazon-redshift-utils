import React, {useEffect} from 'react';
import {Table} from "@awsui/components-react";


export default function ReplayOverview({replays, setReplays}) {

    useEffect(() => {
        const fetchData = async () => {
            const response = await fetch(`/submit_replays`);
            const newData = await response.json();
            setReplays(newData.replays);
        };
        fetchData();
    }, [setReplays]);


    return (
        <Table items={replays} columnDefinitions={COL_DEF}></Table>
    )


};

const COL_DEF = [
        {
            id: 'sid',
            header: 'Replay',
            cell: item => item.sid,
            width: 50
        },

        {
            id: 'id',
            header: 'Cluster',
            cell: item => item.id,
            width: 50
        },
        {
            id: 'status',
            header: 'Status',
            cell: item => item.status,
            width: 50
        },
        {
            id: 'instance',
            header: 'Instance',
            cell: item => item.instance,
            width: 50
        },
        {
            id: 'num_nodes',
            header: 'Nodes',
            cell: item => item.num_nodes,
            width: 50
        },
        {
            id: 'database',
            header: 'Database',
            cell: item => item.database,
            width: 50
        },
        {
            id: 'start_time',
            header: 'Start Time (UTC)',
            cell: item => item.start_time.slice(0, -6),
            width: 50
        },
        {
            id: 'end_time',
            header: 'End Time (UTC)',
            cell: item => item.end_time.slice(0, -6),
            width: 50
        },
        {
            id: 'duration',
            header: 'Duration',
            cell: item => item.duration,
            width: 50
        },
        {
            id: 'query_success',
            header: 'Queries',
            cell: item => item.query_success,
            width: 50
        },
        {
            id: 'connection_success',
            header: 'Connections',
            cell: item => item.connection_success,
            width: 50
        },


    ]