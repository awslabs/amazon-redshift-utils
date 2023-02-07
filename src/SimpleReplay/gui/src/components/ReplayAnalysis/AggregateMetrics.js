import React, {useEffect, useState} from 'react';
import {Header, Table} from "@awsui/components-react";
import millisToMinutesAndSeconds from "../../helpers/msFormatter";


/**
 * Aggregate Metrics Table
 * Displays p-values, averages, standard deviation values for each replay
 */
const AggregateMetrics = ({selectedQueryTypes, selectedUser, selectedDuration}) => {
    /** @prop selectedQueryTypes, array of selected "query type" options */
    /** @prop selectedUser, array of selected "user" options */
    /** @prop selectedDuration, array of selected "duration" range */

    /** Table data */
    const [data, setData] = useState([]);

    /** Loading validator to render component given successful response */
    const [loading, setLoading] = useState(true);


    useEffect(() => {
        const fetchData = async () => {
            fetch(`/agg_metrics?qtype=${JSON.stringify(selectedQueryTypes)}&user=${JSON.stringify(selectedUser)}&start=${(selectedDuration[0])}&end=${(selectedDuration[1])}`).then(response => response.json())
                .then(response => {
                    if (response.success === false) {

                    } else {
                        setData(response.data);
                        setLoading(false);
                    }
                })
                .catch((error) => {
                    console.error('Error:', error);
                });
        };
        fetchData();
    }, [selectedQueryTypes, selectedUser, selectedDuration]);

    /** Render components */
    return !loading && (
        <div>
            <Header
                description={"Percentiles of execution time, elapsed time, and queue time across selected replays." +
                    " These values are representative of the selected query types, users, and time range."}>
                Aggregate Metrics</Header>
            <Table items={data} columnDefinitions={COL_DEF}></Table>
        </div>
    )
};

/** Array of column definitions for Aggregate Metrics table */
const COL_DEF = [
    {
        id: 'sid',
        header: 'Replay',
        cell: item => item.sid,
        width: 50
    },
    {
        id: 'p25',
        header: 'P25 (s)',
        cell: item => millisToMinutesAndSeconds(item.p25, 3),
        width: 50
    },
    {
        id: 'p50',
        header: 'P50 (s)',
        cell: item => millisToMinutesAndSeconds(item.p50, 3),
        width: 50
    },
    {
        id: 'p75',
        header: 'P75 (s)',
        cell: item => millisToMinutesAndSeconds(item.p75, 3),
        width: 50
    },
    {
        id: 'p99',
        header: 'P99 (s)',
        cell: item => millisToMinutesAndSeconds(item.p99, 3),
        width: 50
    },
    {
        id: 'avg',
        header: 'Average (s)',
        cell: item => millisToMinutesAndSeconds(item.avg, 3),
        width: 50
    },
    {
        id: 'std',
        header: 'Standard Deviation (s)',
        cell: item => millisToMinutesAndSeconds(item.std, 3),
        width: 50
    }
]

export default AggregateMetrics;