import React, {useEffect, useState} from 'react';
import {Header, Table} from "@awsui/components-react";
import CopyDiff from "./CopyDiff";


const CopyAgg = ({selectedUser, selectedDuration, replays}) => {

    const [data, setData] = useState([]);
    const [loading, setLoading] = useState(true);

    useEffect(() => {
        const fetchData = async () => {

            fetch(`/copy_agg?user=${JSON.stringify(selectedUser)}&start=${(selectedDuration[0])}&end=${(selectedDuration[1])}`).then(response => response.json())
                .then(response => {
                    if (response.success === false) {
                        console.log(response.message);
                    } else {
                        setData(response.data);
                        setLoading(response.data.length === 0)
                    }
                })
                .catch((error) => {
                    console.error('Error:', error);
                });
        };
        fetchData();
    }, [selectedDuration, selectedUser]);


    return !loading && (
        <div>
            <Header description={"Aggregated execution metrics of COPY ingestion by replay."}>
                COPY Ingestion Metrics</Header>
            <Table items={data} columnDefinitions={COL_DEF}></Table>
            <CopyDiff selectedDuration={selectedDuration} replays={replays}/>

        </div>
    )
};

const COL_DEF = [
    {
        id: 'replay',
        header: 'Replay',
        cell: item => item.sid,
        width: 50,
    },
    {
        id: 'loadedRows',
        header: 'Loaded Rows',
        cell: item => item.loaded_rows,
        width: 50,
        maxWidth: 300
    },
    {
        id: 'loadedBytes',
        header: 'Loaded Bytes',
        cell: item => item.loaded_bytes,
        width: 50
    },
    {
        id: 'sourceFileCount',
        header: 'Source File Count',
        cell: item => item.source_file_count,
        width: 50
    }

]

export default CopyAgg;