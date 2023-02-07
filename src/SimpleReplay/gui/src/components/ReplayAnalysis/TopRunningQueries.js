import React, {useEffect, useState} from 'react';
import {Box, Button, Input, Header, Multiselect, Pagination, SpaceBetween, Table} from "@awsui/components-react";
import {useCollection} from "@awsui/collection-hooks";
import millisToMinutesAndSeconds from "../../helpers/msFormatter";
import prepareSelectOptions from "../../helpers/PrepareOptions";


const TopRunningQueries = ({selectedQueryTypes, selectedUser, selectedDuration}) => {

    const [data, setData] = useState([]);
    const selectReplayOptions = prepareSelectOptions('sid', data);
    const [loading, setLoading] = useState(true);
    const [selectedReplays, setSelectedReplays] = useState([]);

    useEffect(() => {
        const fetchData = async () => {
            fetch(`/top_queries`).then(response => response.json())
                .then(response => {
                    if (response.success === false) {
                    } else {
                        setData(response.data)
                        setLoading(false)
                    }
                })

                .catch((error) => {
                    console.error('Error:', error);

                });
        };
        fetchData();
    }, []);

    /**
     * FILTERING FUNCTIONS: all have the same structure
     * @param {Object} item individual entry in collection
     * @return {boolean} whether item matches filter criteria
     */
    function matchesReplay(item, replays) {
        if (replays.length === 0) {
            return true
        }
        return replays.some(rep => rep.label === item.sid)
    }

    function matchesQueryType(item) {
        if (selectedQueryTypes.length === 0) {
            return true
        }
        return selectedQueryTypes.some(rep => rep.label === item.query_type)
    }

    function matchesUser(item) {
        if (selectedUser.length === 0) {
            return true
        }
        return selectedUser.some(rep => rep.label === item.user_name)
    }

    function matchesDuration(item) {
        return (item.start_diff >= selectedDuration[0] && item.start_diff <= selectedDuration[1]) || (item.end_diff >= selectedDuration[0] && item.end_diff <= selectedDuration[1])
    }

    function clearFilter() {
        actions.setFiltering('');
        setSelectedReplays([]);
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

                    if (selectedReplays.length === 0 && selectedUser.length === 0 && selectedQueryTypes.length === 0 && filteringText === "") {
                        if (selectedDuration === [0, 0]) {
                            return true;
                        }
                        return matchesDuration(item);
                    }

                    if (!matchesReplay(item, selectedReplays) && !matchesUser(item) && !matchesQueryType(item) && !matchesDuration(item)) {
                        return false;
                    }

                    return (matchesReplay(item, selectedReplays) && matchesQueryType(item) && matchesUser(item) && matchesDuration(item) && textMatch)
                },

            },
            pagination: {},
            sorting: {},
            selection: {},

        });


    return !loading && (
        <div>
            <Header description={"Queries in order of execution time."}>
                Longest Running Queries</Header>
            <Table
                {...collectionProps}
                items={items}
                columnDefinitions={COL_DEF}
                pagination={<Pagination {...paginationProps}/>}
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
                            <Multiselect hideTokens
                                         selectedOptions={selectedReplays}
                                         options={selectReplayOptions}
                                         placeholder={"Filter by replay"}
                                         onChange={({detail}) =>
                                             setSelectedReplays(detail.selectedOptions)
                                         }/>
                            <Button
                                onClick={() => {
                                    clearFilter()
                                }}
                                variant={"normal"}
                                disabled={selectedReplays.length === 0 && filterProps.filteringText === ""}

                            >Clear Filters</Button>
                            <Button
                                variant={"normal"}
                                href={"/analysis"}
                                disabled={false}
                                iconName={"download"}
                                onClick={() => {

                                }}> Download</Button>


                        </SpaceBetween>
                    </div>
                }

            />
        </div>
    )
};

const SEARCHABLE_COLUMNS = ['sid', 'user_name', 'query_text', 'query_type', 'query_id', 'start_time'];
    const COL_DEF = [
        {
            id: 'sid',
            header: 'Replay',
            cell: item => item.sid,
            width: 50
        },
        {
            id: 'query_id',
            header: 'Query ID',
            cell: item => item.query_id,
            width: 50
        },
        {
            id: 'start_time',
            header: 'Start Time',
            cell: item => item.start_time.slice(0, -4),
            width: 50
        },
        //   {
        //     id: 'end_time',
        //     header: 'End Time',
        //     cell: item => item.end_time,
        //     width: 50
        // },
        {
            id: 'user_name',
            header: 'User',
            cell: item => item.user_name,
            width: 50
        },
        {
            id: 'database',
            header: 'DB',
            cell: item => item.database_name,
            width: 50
        },
        // {
        //     id: 'query_type',
        //     header: 'Query',
        //     cell: item => item.query_type,
        //     width: 50
        // },
        {
            id: 'execution_time',
            header: 'Execution Time',
            cell: item => millisToMinutesAndSeconds(item.execution_time),
            width: 50
        },
        {
            id: 'queue_time',
            header: 'Queue Time',
            cell: item => millisToMinutesAndSeconds(item.queue_time),
            width: 50
        },
        {
            id: 'elapsed_time',
            header: 'Elapsed Time',
            cell: item => millisToMinutesAndSeconds(item.elapsed_time),
            width: 50
        },
        {
            id: 'query_text',
            header: 'Query Text',
            cell: item => item.query_text,
            maxWidth: 300
        },

    ]

export default TopRunningQueries;