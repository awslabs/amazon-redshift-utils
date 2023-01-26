import React, {useState} from 'react';
import {Box, Button, Header, Input, Pagination, Select, SpaceBetween, Table} from "@awsui/components-react";
import prepareSelectOptions from "../../helpers/PrepareOptions";
import millisToMinutesAndSeconds from "../../helpers/msFormatter";
import {useCollection} from "@awsui/collection-hooks";


const QueryDeltas = ({selectedQueryTypes, selectedUser, selectedDuration, replays}) => {

    const [data, setData] = useState([]);

    /** Variables for the currently selected value for baseline/target selectors */
    const [selectedBaseline, setSelectedBaseline] = useState({label: 'Baseline', value: 'Baseline'});
    const [selectedTarget, setSelectedTarget] = useState({label: 'Target', value: 'Target'});

    /** Populate list of possible replays */
    const selectReplayOptions = prepareSelectOptions('sid', replays);

    /** TODO: configure user input to set threshold for magnitude difference */
    const [magnitudeThreshold, setMagnitudeThreshold] = useState("00:00:01");


    function resetReplays() {
        setSelectedBaseline({label: 'Baseline', value: 'Baseline'});
        setSelectedTarget({label: 'Target', value: 'Target'});
        setData([])
    }

    function clearFilter() {
        actions.setFiltering('');
    }

    function targetOptions() {
        return selectReplayOptions.filter(value => value.label !== selectedBaseline.label)
    }

    function matchesDuration(item) {
        const start = (item.start_diff_b >= selectedDuration[0] && item.start_diff_b <= selectedDuration[1]) || (item.start_diff_t >= selectedDuration[0] && item.start_diff_t <= selectedDuration[1]);
        const end = (item.end_diff_b >= selectedDuration[0] && item.end_diff_b <= selectedDuration[1]) || (item.end_diff_t >= selectedDuration[0] && item.end_diff_t <= selectedDuration[1]);
        return start || end
    }

    function matchesUser(item) {
        if (selectedUser.length === 0) {
            return true
        }
        return selectedUser.some(rep => rep.label === item.user_name_b)
    }

    function matchesQueryType(item) {
        if (selectedQueryTypes.length === 0) {
            return true
        }
        return selectedQueryTypes.some(rep => rep.label === item.query_type_b)
    }

    function matchesThreshold(item) {
        if (magnitudeThreshold.length === 0 || magnitudeThreshold === "00:00:00") {
            return true
        }
        if (typeof magnitudeThreshold !== "string") return true
        const thresholdms = 1000 * (parseInt(magnitudeThreshold.slice(0, 2)) * 3600 + parseInt(magnitudeThreshold.slice(3, 5)) * 60 + parseInt(magnitudeThreshold.slice(6, 8)));

        return Math.abs(item.execution_time_b - item.execution_time_t) >= thresholdms || Math.abs(item.elapsed_time_b - item.elapsed_time_t) >= thresholdms
    }

    function compare(label) {
        if (label !== "Target") {
            fetch(`/perf_diff?baseline=${selectedBaseline.label}&target=${label}`).then(response => response.json())
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

                    if (filteringText === "") {
                        textMatch = true;
                    }

                    if (selectedUser.length === 0 && selectedQueryTypes.length === 0 && filteringText === "") {
                        if (selectedDuration === [0, 0]) {
                            return true;
                        }
                        return matchesDuration(item) && matchesThreshold(item);
                    }

                    if (!matchesDuration(item) && !matchesUser(item) && !matchesQueryType(item) && !matchesThreshold(item)) {
                        return false;
                    }

                    return (matchesDuration(item) && matchesQueryType(item) && matchesUser(item) && matchesThreshold(item) && textMatch)
                },

            },
            pagination: {},
            sorting: {},
            selection: {},

        });


    return (
        <div>
            <Header description={"Queries with the greatest difference in execution time across selected replays."}>
                Top Query Execution Deltas</Header>
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

                               >Reset Comparison</Button>

                               {/*TODO: Magnitude threshold user input*/}
                               {/*<TimeInput*/}
                               {/*    value={magnitudeThreshold}*/}
                               {/*    onChange={({detail}) => {*/}
                               {/*        if(detail.value.length === 8) {*/}
                               {/*            setMagnitudeThreshold(detail.value);*/}
                               {/*        }*/}

                               {/*    }}*/}
                               {/*    placeholder={`hh:mm:ss`}*/}
                               {/*  />*/}


                           </SpaceBetween>
                       </div>
                   }
            ></Table>
        </div>
    )
};

const SEARCHABLE_COLUMNS = ['user_name_b', 'query_text_b', 'query_type_b'];

const execBool = (item) => {
    if (typeof (item.status_b) === "object" || typeof (item.status_t) === "object") {
        return String(false);
    } else {
        return String(item.status_b === "success" && item.status_t === "success")
    }
}

const COL_DEF = [
    {
        id: 'verifier',
        sortingField: item => execBool(item),
        header: 'Success',
        cell: item => execBool(item),
        width: 50,

    },
    {
        id: 'queryText',
        header: 'Query Text',
        cell: item => item.query_text_b,
        width: 50,
        maxWidth: 300
    },
    //   {
    //     id: 'queryType',
    //     header: 'Query Type',
    //     cell: item => item.query_type_b,
    //     width: 50,
    //     maxWidth: 300
    // },
    //   {
    //     id: 'username',
    //     header: 'User',
    //     cell: item => item.user_name_b,
    //     width: 50,
    // },
    {
        id: 'b_elapsed',
        header: 'B: Elapsed Time',
        cell: item => millisToMinutesAndSeconds(item.elapsed_time_b, 4),
        width: 50
    },
    {
        id: 't_elapsed',
        header: 'T: Elapsed Time',
        cell: item => millisToMinutesAndSeconds(item.elapsed_time_t, 4),
        width: 50
    },
    {
        id: 'elapseddiff',
        header: 'Difference',
        cell: item => Math.abs(Math.round(item.elapsed_diff * 100) / 100) + "%",
        width: 50
    },
    {
        id: 'b_exec',
        header: 'B: Execution Time',
        cell: item => millisToMinutesAndSeconds(item.execution_time_b, 4),
        width: 50
    },
    {
        id: 't_exec',
        header: 'T: Execution Time',
        cell: item => millisToMinutesAndSeconds(item.execution_time_t, 4),
        width: 50
    },
    {
        id: 'execdiff',
        header: 'Difference',
        cell: item => Math.abs(Math.round(item.exec_diff * 100) / 100) + "%",
        width: 50
    },

];
export default QueryDeltas;