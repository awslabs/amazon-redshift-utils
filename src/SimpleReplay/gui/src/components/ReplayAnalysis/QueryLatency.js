import React, {useEffect, useState} from 'react';
import {BarChart, Box, Button, Header} from "@awsui/components-react";
import millisToMinutesAndSeconds from "../../helpers/msFormatter";


/**
 * Compare Throughput Chart
 * Displays p-values, averages, standard deviation values for each replay
 */

const QueryLatency = ({selectedQueryTypes, selectedUser, selectedDuration}) => {
    /** @prop selectedQueryTypes, array of selected "query type" options */
    /** @prop selectedUser, array of selected "user" options */
    /** @prop selectedDuration, array of selected "duration" range */

    /** Series data */
    const [data, setData] = useState([]);

    /** Loading validator to render component given successful response */
    const [loading, setLoading] = useState(true);

    useEffect(() => {
        const fetchData = async () => {
            fetch(`/query_latency?qtype=${JSON.stringify(selectedQueryTypes)}&user=${JSON.stringify(selectedUser)}&start=${(selectedDuration[0])}&end=${(selectedDuration[1])}`).then(response => response.json())
                .then(response => {
                    if (response.success === false) {
                    } else {
                        setData(response.data.map((entry) =>
                            ({
                                title: entry.replay,
                                type: "bar",
                                data: entry.values.map((val) => ({x: (val.bin), y: (val.count)}))
                            })
                        ))
                        setLoading(false)
                    }

                })

                .catch((error) => {
                    console.error('Error:', error);

                })
        };

        fetchData();
    }, [selectedQueryTypes, selectedUser, selectedDuration]);


    return !loading && (
        <div>
            <Header description={"Distribution of query latency."}>
                Query Latency</Header>
            <BarChart
                series={data}

                i18nStrings={{
                    filterLabel: "Filter displayed data",
                    filterPlaceholder: "Filter data",
                    filterSelectedAriaLabel: "selected",
                    legendAriaLabel: "Legend",
                    chartAriaRoleDescription: "bar chart",
                    xTickFormatter: e =>
                        millisToMinutesAndSeconds(e, 1)
                }}
                errorText="Error loading data."
                height={300}
                loadingText="Loading chart"
                recoveryText="Retry"
                xScaleType="categorical"
                xTitle="Elapsed Time"
                yTitle="# of Queries"
                empty={
                    <Box textAlign="center" color="inherit">
                        <b>No data available</b>
                        <Box variant="p" color="inherit">
                            There is no data available
                        </Box>
                    </Box>
                }
                noMatch={
                    <Box textAlign="center" color="inherit">
                        <b>No matching data</b>
                        <Box variant="p" color="inherit">
                            There is no matching data to display
                        </Box>
                        <Button>Clear filter</Button>
                    </Box>
                }
            />
        </div>
    );
};

export default QueryLatency;