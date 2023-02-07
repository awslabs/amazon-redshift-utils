import React, {useEffect, useState} from 'react';
import {Box, Button, Header, LineChart} from "@awsui/components-react";
import millisToMinutesAndSeconds from "../../helpers/msFormatter";

/**
 * Compare Throughput Chart
 * Displays p-values, averages, standard deviation values for each replay
 */
const CompareThroughput = ({selectedQueryTypes, selectedDuration, selectedUser}) => {
    /** @prop selectedQueryTypes, array of selected "query type" options */
    /** @prop selectedUser, array of selected "user" options */
    /** @prop selectedDuration, array of selected "duration" range */

    /** Series data */
    const [data, setData] = useState([]);

    /** Loading validator to render component given successful response */
    const [loading, setLoading] = useState(true);

    useEffect(() => {
        const fetchData = async () => {
            fetch(`/compare_throughput?qtype=${JSON.stringify(selectedQueryTypes)}&user=${JSON.stringify(selectedUser)}`).then(response => response.json())
                .then(response => {
                    if (response.success === false) {
                        console.log(response.message);
                    } else {
                        /** Maps response data to LineChart formatting */
                        setData(response.data.map((entry) =>
                            ({
                                title: entry.replay,
                                type: "line",
                                data: entry.values.map((val) =>
                                    ({x: (val.rel_time), y: val.freq}))
                            })));

                        setLoading(false);
                    }

                })
                .catch((error) => {
                    console.error('Error:', error);

                });
        };
        fetchData();
    }, [selectedQueryTypes, selectedUser]);

    /**
     * Filters a series by given duration range
     * @param {Object} series Total data set of query frequency values.
     * @return {Object} filtered data set on duration
     */
    function filterRange(series) {
        return series.map(singleSerie => ({
            ...singleSerie,
            data: singleSerie.data.filter(value => value.x >= selectedDuration[0] && value.x <= selectedDuration[1])
        }));
    }

    return !loading && (
        <div>
            <Header
                description={"Total number of queries executed per second. This data is filtered by the selected query types, users, and time range."}>
                Compare Throughput</Header>

            <LineChart
                series={filterRange(data)}
                hideFilter={true}
                height={300}
                statusType={loading ? "loading" : "finished"}
                i18nStrings={{
                    filterLabel: "Filter by replay",
                    filterPlaceholder: "Filter data",
                    filterSelectedAriaLabel: "selected",
                    legendAriaLabel: "Legend",
                    chartAriaRoleDescription: "line chart",
                    xTickFormatter: e =>
                        millisToMinutesAndSeconds(e, 0)
                }}
                xScaleType={'linear'}
                xTitle={'Timestamp (relative to start time)'}
                yTitle={'Queries Executed'}
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
                loadingText={"Loading"}
            ></LineChart>
        </div>

    );
};

export default CompareThroughput