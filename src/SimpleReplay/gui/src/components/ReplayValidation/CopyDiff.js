import React, {useState} from 'react';
import {Box, Header, Pagination, Select, SpaceBetween, Table} from "@awsui/components-react";
import Input from "@awsui/components-react/input";
import Button from "@awsui/components-react/button";
import prepareSelectOptions from "../../helpers/PrepareOptions";
import {useCollection} from "@awsui/collection-hooks";


const CopyDiff = ({selectedDuration, replays}) => {

    const [data, setData] = useState([]);
    const [selectedBaseline, setSelectedBaseline] = useState({label: 'Baseline', value: 'Baseline'});
    const [selectedTarget, setSelectedTarget] = useState({label: 'Target', value: 'Target'});
    const selectReplayOptions = prepareSelectOptions('sid', replays);


    function resetReplays() {
        setSelectedBaseline({label: 'Baseline', value: 'Baseline'});
        setSelectedTarget({label: 'Target', value: 'Target'});
        setData([])
    }

    function clearFilter() {
    }

    function targetOptions() {
        return selectReplayOptions.filter(value => value.label !== selectedBaseline.label)
    }

    function matchesDuration(item) {
        const start = (item.start_diff_x_b >= selectedDuration[0] && item.start_diff_x_b <= selectedDuration[1])
            || (item.start_diff_x_t >= selectedDuration[0] && item.start_diff_x_t <= selectedDuration[1])
            || (item.start_diff_y_b >= selectedDuration[0] && item.start_diff_y_b <= selectedDuration[1])
            || (item.start_diff_y_t >= selectedDuration[0] && item.start_diff_y_t <= selectedDuration[1]);
        const end = (item.end_diff_x_b >= selectedDuration[0] && item.end_diff_x_b <= selectedDuration[1])
            || (item.end_diff_x_t >= selectedDuration[0] && item.end_diff_x_t <= selectedDuration[1])
            || (item.end_diff_y_b >= selectedDuration[0] && item.end_diff_y_b <= selectedDuration[1])
            || (item.end_diff_y_t >= selectedDuration[0] && item.end_diff_y_t <= selectedDuration[1]);

        return start || end
    }

    function compare(targetLabel) {
        if (targetLabel !== "Target") {
            fetch(`/copy_diff?baseline=${selectedBaseline.label}&target=${targetLabel}`).then(response => response.json())
                .then(response => {
                    if (response.success === false) {
                        console.log(response.message);
                    } else {
                        setData(response.data)
                    }
                })

                .catch((error) => {
                    console.error('Error:', error);

                });
        }
    }

    const {items, actions, filteredItemsCount, collectionProps, filterProps, paginationProps} = useCollection(data,
        {
            filtering: {
                empty: (<Box textAlign="center" color="inherit">
                    <b>No resources</b>
                    <Box
                        padding={{bottom: "s"}}
                        variant="p"
                        color="inherit"
                    >
                        No data to display.
                    </Box>
                </Box>),
                noMatch: (<Box textAlign="center" color="inherit">
                    <b>No data</b>
                    <Box
                        padding={{bottom: "s"}}
                        variant="p"
                        color="inherit"
                    >
                        No matches to display.
                    </Box>
                    <Button onClick={() => clearFilter()}>Clear table filters</Button>
                </Box>),
                filteringFunction: (item, filteringText) => {
                    const filteringTextLowerCase = filteringText.toLowerCase();

                    let textMatch = SEARCHABLE_COLUMNS.map(key => item[key]).some(
                        value => typeof value === 'string' && value.toLowerCase().indexOf(filteringTextLowerCase) > -1
                    );

                    let bool = execBool(item)
                    if (filteringTextLowerCase === bool) {
                       textMatch = true
                    }

                    if (filteringText === "") {
                        textMatch = true;
                    }

                    if (selectedDuration === [0, 0]) {
                        return true;
                    }

                    if (!matchesDuration(item)) {
                        return false;
                    }

                    return (matchesDuration(item) && textMatch)
                },

            },
            pagination: {},
            sorting: {},
            selection: {},

        });


    return (
        <div>
            <Header description={"Differences in COPY ingestion at the query level."}>
                COPY Ingestion Deltas</Header>
            <Table {...collectionProps}
                   items={items}
                   columnDefinitions={COL_DEF}
                   pagination={<Pagination {...paginationProps}/>}
                   empty={
                       <Box textAlign="center" color="inherit">
                           <b>Select replays</b>
                           <Box
                               padding={{bottom: "s"}}
                               variant="p"
                               color="inherit"
                           >
                               Use the selection filters to choose a "baseline" and "target" replay to compare
                               differences in performance.
                           </Box>
                       </Box>
                   }
                   filter={
                       <div className="input-container">
                           <SpaceBetween size={"s"} direction={"horizontal"}>
                               <Input
                                   type="search"
                                   value={filterProps.filteringText}
                                   onChange={event => {
                                        actions.setFiltering(event.detail.value);
                                   }}
                                   placeholder="Find text..."
                               />
                               <Select
                                   selectedOption={selectedBaseline}
                                   onChange={({detail}) =>
                                       setSelectedBaseline(detail.selectedOption)

                                   }
                                   options={selectReplayOptions}
                                   selectedAriaLabel="Selected"
                               />

                               <Select
                                   selectedOption={selectedTarget}
                                   onChange={({detail}) => {
                                       setSelectedTarget(detail.selectedOption);
                                       compare(detail.selectedOption.label);

                                   }}
                                   options={targetOptions()}
                                   disabled={selectedBaseline.label === "Baseline"}
                                   selectedAriaLabel="Selected"
                               />

                               <Button
                                   onClick={() => {
                                       resetReplays()
                                   }}
                                   variant={"normal"}
                                   disabled={selectedBaseline.label === "Baseline"}

                               >Reset</Button>


                           </SpaceBetween>
                       </div>
                   }
            ></Table>
        </div>
    )
};


const SEARCHABLE_COLUMNS = ["data_source_b",
    "data_source_t",
    "database_name_x_b",
    "database_name_x_t",
    "database_name_y_b",
    "database_name_y_t",
    "error_message_b",
    "error_message_t",
    "query_label_b",
    "query_label_t",
    "query_text_b",
    "query_text_t",
    "query_type_b",
    "query_type_t",
    "redshift_version_b",
    "redshift_version_t",
    "result_cache_hit_b",
    "result_cache_hit_t",
    "status_x_b",
    "status_x_t",
    "status_y_b",
    "status_y_t",
    "table_name_b",
    "table_name_t",
    "user_name_b",
    "user_name_t"
];

    const execBool = (item) => {
        if (typeof (item.status_x_b) === "object" || typeof (item.status_x_t) === "object") {
            return String(false);
        } else {
            return String(item.status_x_b === "success" && item.status_x_t === "success")
        }
    }

    const COL_DEF = [
        {
            id: 'loadedBoth',
            header: 'Success',
            cell: item => execBool(item),
            width: 50,
            sortingField: item => execBool(item),

        },
        {
            id: 'baselineStatus',
            header: 'Baseline',
            cell: item => item.status_x_b ? item.status_x_b : "-",
            width: 50
        },
        {
            id: 'targetStatus',
            header: 'Target',
            cell: item => item.status_x_t ? item.status_x_t : "-",
            width: 50
        },
        {
            id: 'table',
            header: 'Table',
            cell: item => item.table_name_b ? item.table_name_b : "-",
            width: 50,
            maxWidth: 300
        },
        {
            id: 'b_loadedBytes',
            header: 'B: Loaded Bytes',
            cell: item => item.loaded_bytes_b ? item.loaded_bytes_b : 0,
            width: 50
        },
        {
            id: 't_loadedBytes',
            header: 'T: Loaded Bytes',
            cell: item => item.loaded_bytes_t ? item.loaded_bytes_t : 0,
            width: 50
        },
        {
            id: 'source_file_count_b',
            header: 'B: Source File Count',
            cell: item => item.source_file_count_b ? item.source_file_count_b : 0,
            width: 50
        },
        {
            id: 'source_file_count_t',
            header: 'T: Source File Count',
            cell: item => item.source_file_count_t ? item.source_file_count_t : 0,
            width: 50
        },
        {
            id: 'error',
            header: 'Error',
            cell: item => item.error_message_b || item.error_message_t ? item.error_message_b || item.error_message_t : "-",
            width: 50,
            maxWidth: 300
        },
        {
            id: 'dataSource',
            header: 'Source',
            cell: item => item.data_source_b ? item.data_source_b : "-",
            width: 50,
            maxWidth: 300
        }
    ]

export default CopyDiff;