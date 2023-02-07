import React, {useEffect, useState} from 'react';
import {
    Box,
    Button,
    CollectionPreferences,
    Header,
    Input,
    Multiselect,
    Pagination,
    SpaceBetween,
    Table
} from "@awsui/components-react";
import {useCollection} from "@awsui/collection-hooks";
import prepareSelectOptions from "../../helpers/PrepareOptions";


const ErrorTable = ({selectedDuration, selectedUser}) => {

    const [data, setData] = useState([]);
    const selectReplayOptions = prepareSelectOptions('sid', data);
    const selectCategoryOptions = prepareSelectOptions('category', data);
    const [loading, setLoading] = useState(true);
    const [replayPlaceholder, setReplayPlaceholder] = useState("Filter by replay");
    const [categoryPlaceholder, setCategoryPlaceholder] = useState("Filter by error class");
    const [selectedReplays, setSelectedReplays] = useState([]);
    const [selectedClasses, setSelectedClasses] = useState([]);

    useEffect(() => {
        const fetchData = async () => {
            fetch(`/err_table`).then(response => response.json())
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

    useEffect(() => {
        if (selectedReplays.length !== 0) {
            setReplayPlaceholder(`${selectedReplays.length} selected`)
        } else {
            setReplayPlaceholder("Filter by replay")
        }
        if (selectedClasses.length !== 0) {
            setCategoryPlaceholder(`${selectedClasses.length} selected`)
        } else {
            setCategoryPlaceholder("Filter by class")
        }
    }, [selectedClasses.length, selectedReplays.length]);


    function matchesCategory(item, categories) {
        if (categories.length === 0) {
            return true
        }
        return categories.some(cat => cat.label === item.category)
    }

    function matchesReplay(item, replays) {
        if (replays.length === 0) {
            return true
        }
        return replays.some(rep => rep.label === item.sid)
    }

    function matchesUser(item) {
        if (selectedUser.length === 0) {
            return true
        }
        return selectedUser.some(rep => rep.label === item.user_name)
    }

    function matchesDuration(item) {
        return (item.time_diff >= selectedDuration[0] && item.time_diff <= selectedDuration[1])
    }

    function clearFilter() {
        actions.setFiltering('');
        setSelectedClasses([]);
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
                    <b>No resources</b>
                    <Box
                        padding={{bottom: "s"}}
                        variant="p"
                        color="inherit"
                    >
                        No matches to display.
                    </Box>
                    <Button onClick={() => clearFilter()}>Clear filters</Button>
                </Box>),

                filteringFunction: (item, filteringText) => {
                    const filteringTextLowerCase = filteringText.toLowerCase();

                    let textMatch = SEARCHABLE_COLUMNS.map(key => item[key]).some(
                        value => typeof value === 'string' && value.toLowerCase().indexOf(filteringTextLowerCase) > -1
                    );

                    if (filteringText === "") {
                        textMatch = true;
                    }

                    if (selectedClasses.length === 0 && selectedUser.length === 0 && selectedReplays.length === 0 && filteringText === "") {
                        if (selectedDuration === [0, 0]) {
                            return true;
                        }
                        return matchesDuration(item);
                    }

                    if (!matchesReplay(item, selectedReplays) && !matchesUser(item) && !matchesCategory(item, selectedClasses) && !matchesDuration(item)) {
                        return false;
                    }

                    return (matchesReplay(item, selectedReplays) && matchesUser(item) && matchesCategory(item, selectedClasses) && matchesDuration(item) && textMatch)
                },
            },
            pagination: {},
            sorting: {},
            selection: {},

        });


    return !loading && (
        <div>
            <Header description={"Errors encountered across selected replays."}>
                Query Errors</Header>
            <Table
                {...collectionProps}
                items={items}
                columnDefinitions={COL_DEF}

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
                                         ariaLabelledby={"Hey"}
                                         selectedOptions={selectedReplays}
                                         options={selectReplayOptions}
                                         placeholder={replayPlaceholder}
                                         onChange={({detail}) =>
                                             setSelectedReplays(detail.selectedOptions)
                                         }/>
                            <Multiselect hideTokens
                                         selectedOptions={selectedClasses}
                                         placeholder={categoryPlaceholder}
                                         options={selectCategoryOptions}
                                         onChange={({detail}) =>
                                             setSelectedClasses(detail.selectedOptions)
                                         }/>
                            <Button
                                onClick={() => {
                                    clearFilter()
                                }}
                                variant={"normal"}
                                disabled={selectedClasses.length === 0 && selectedReplays.length === 0 && filterProps.filteringText === ""}

                            >Clear Filters</Button>

                        </SpaceBetween>
                    </div>
                }
                pagination={<Pagination {...paginationProps}/>}
                preferences={<CollectionPreferences title={"Table Preferences"} confirmLabel={"Confirm"}
                                                    cancelLabel={"Cancel"}/>}
            ></Table>
        </div>
    )
};

const SEARCHABLE_COLUMNS = ['sid', 'user', 'db', 'code', 'category', 'message', 'query_text'];

const COL_DEF = [
    {
        id: 'sid',
        header: 'Replay',
        cell: item => item.sid,
        width: 50
    },
    {
        id: 'timestamp',
        header: 'Timestamp',
        cell: item => item.timestamp,
        width: 50
    },
    {
        id: 'user',
        header: 'User',
        cell: item => item.user,
        width: 50
    },
    {
        id: 'db',
        header: 'Database',
        cell: item => item.db,
        width: 50
    },
    {
        id: 'code',
        header: 'Error Code',
        cell: item => item.code,
        width: 50
    },

    {
        id: 'category',
        header: 'Error Class',
        cell: item => item.category,
        width: 50
    },
    {
        id: 'message',
        header: 'Error Message',
        cell: item => item.message,
        maxWidth: 300
    },
    {
        id: 'query_text',
        header: 'Query Text',
        cell: item => item.query_text,
        maxWidth: 300
    },
    {
        id: 'detail',
        header: 'Detail',
        cell: item => item.detail ? item.detail : "-",
        maxWidth: 300
    }
]

export default ErrorTable