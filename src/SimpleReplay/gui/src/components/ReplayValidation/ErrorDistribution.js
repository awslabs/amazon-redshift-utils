import React, {useEffect, useState} from 'react';
import {BarChart, Box, Header} from "@awsui/components-react";
import Button from "@awsui/components-react/button";

const ErrorDistribution = () => {

    const [data, setData] = useState([]);
    const [loading, setLoading] = useState(true);

    useEffect(() => {
        const fetchData = async () => {
            fetch(`/err_distribution`).then(response => response.json())
                .then(response => {
                    if (response.success === false) {
                    } else {
                        setData(response.data.map((entry) =>
                            ({
                                title: entry.replay,
                                type: "bar",
                                data: entry.values.map((val) =>
                                    ({x: (val.category), y: val.freq}))
                            })))
                        setLoading(false)
                    }

                })

                .catch((error) => {
                    console.error('Error:', error);

                });
        };
        fetchData();
    }, []);


    return !loading && (
        <div>
            <Header description={"Frequency of errors across selected replays. Not filtered on any filter criteria."}>
                Error Category Distribution</Header>
            <BarChart
                series={data}

                i18nStrings={{
                    filterLabel: "Filter displayed data",
                    filterPlaceholder: "Filter data",
                    filterSelectedAriaLabel: "selected",
                    legendAriaLabel: "Legend",
                    chartAriaRoleDescription: "bar chart"
                }}
                ariaLabel="Multiple data series line chart"
                errorText="Error loading data."
                height={300}
                loadingText="Loading chart"
                recoveryText="Retry"
                xScaleType="categorical"
                xTitle="Error Category"
                yTitle="Queries"
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

export default ErrorDistribution;