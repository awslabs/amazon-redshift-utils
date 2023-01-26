import React from 'react';
import {AreaChart, Box} from "@awsui/components-react";
import Button from "@awsui/components-react/button";

/** COMPONENT NOT IN USE */
/** TODO: Format request for breakdown chart */

const ThroughputBreakdown = () => {
    return (
        <AreaChart
            series={[]}
            xDomain={[
                new Date(1601017200000),
                new Date(1601045100000)
            ]}
            yDomain={[0, 1]}
            i18nStrings={{
                filterLabel: "Filter displayed data",
                filterPlaceholder: "Filter data",
                filterSelectedAriaLabel: "selected",
                legendAriaLabel: "Legend",
                chartAriaRoleDescription: "line chart",
                detailTotalLabel: "Total",
                xTickFormatter: e =>
                    e
                        .toLocaleDateString("en-US", {
                            month: "short",
                            day: "numeric",
                            hour: "numeric",
                            minute: "numeric",
                            hour12: !1
                        })
                        .split(",")
                        .join("\n"),
                yTickFormatter: function o(e) {
                    return (100 * e).toFixed(0) + "%";
                }
            }}
            ariaLabel="Stacked area chart, multiple metrics"
            errorText="Error loading data."
            height={200}
            loadingText="Loading chart"
            recoveryText="Retry"
            xScaleType="time"
            xTitle="Time (UTC)"
            yTitle="Total CPU load"
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
    );
};

export default ThroughputBreakdown;