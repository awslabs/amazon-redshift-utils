import React, {useEffect, useState} from 'react';
import {Box, Header, Pagination, Table} from "@awsui/components-react";
import {useCollection} from "@awsui/collection-hooks";
import Button from "@awsui/components-react/button";
import Input from "@awsui/components-react/input";

const COL_DEF = [
    {
        id: 'replay_id',
        sortingField: 'replay_id',
        header: 'Replay ID',
        cell: item => item.replay_id,
        width: 50
    },
    {
        id: 'bucket',
        sortingField: 'bucket',
        header: 'Bucket Name',
        cell: item => item.bucket,
        width: 50
    },
    {
        id: 'id',
        sortingField: 'id',
        header: 'Cluster',
        cell: item => item.id,
        width: 50
    },
    {
        id: 'replay_tag',
        sortingField: 'replay_tag',
        header: 'Tag',
        cell: item => item.replay_tag,
        width: 50
    },

    {
        id: 'instance',
        sortingField: 'instance',
        header: 'Instance',
        cell: item => item.instance,
        width: 50
    },
    {
        id: 'num_nodes',
        header: 'Nodes',
        cell: item => item.num_nodes,
        width: 50
    },
    {
        id: 'start_time',
        sortingField: 'start_time',
        header: 'Start Time',
        cell: item => item.start_time,
        width: 50
    },
    {
        id: 'end_time',
        sortingField: 'end_time',
        header: 'End Time',
        cell: item => item.end_time,
        width: 50
    },
]
const SEARCHABLE_COLUMNS = ['replay_id', 'replay_tag', 'bucket', 'instance'];


export default function ReplayList({search, replays}) {

    const [count, setCount] = useState(0);
    const [selectedItems, setSelectedItems] = useState([]);
    const [loading, setLoading] = useState(false);
    const [disabled, setDisabled] = useState(false);

    useEffect(() => {
        setCount(replays ? replays.length : 0)
        setLoading(search);
        if (selectedItems.length === 0) {
            setDisabled(true)

        } else {
            setDisabled(false);
        }
    }, [replays, search, count, selectedItems]);


    const postReplays = () => {
        fetch(`/submit_replays`, {
            'method': 'POST',
            headers: {
                'Content-Type': 'application/json'
            },
            body: JSON.stringify(selectedItems)
        })
            .then(_ => {
                window.location.href = "/analysis"
            })
            .catch(error => console.log(error))
    }


    const {items, actions, filteredItemsCount, collectionProps, filterProps, paginationProps} = useCollection(replays,
        {
            filtering: {
                empty: (<Box textAlign="center" color="inherit">
                    <b>No replays</b>
                    <Box
                        padding={{bottom: "s"}}
                        variant="p"
                        color="inherit"
                    >
                        No replays to display. Use the search bar above to locate replays. Ensure the provided bucket
                        has replay analysis logs.
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
                    <Button onClick={() => actions.setFiltering("")}>Clear filters</Button>
                </Box>),
                filteringFunction: (item, filteringText) => {
                    const filteringTextLowerCase = filteringText.toLowerCase();
                    return SEARCHABLE_COLUMNS.map(key => item[key]).some(
                        value => typeof value === 'string' && value.toLowerCase().indexOf(filteringTextLowerCase) > -1
                    );
                },
            },
            pagination: {},
            sorting: {},
            selection: {},

        });


    return (
        <Table
            {...collectionProps}
            header={
                <Header
                    actions={
                        <Button
                            variant={"primary"}
                            disabled={disabled}
                            onClick={() => {
                                postReplays()
                            }}>Analysis</Button>}
                    counter={`(${count})`}>Replays</Header>
            }
            onSelectionChange={({detail}) =>
                setSelectedItems(detail.selectedItems)
            }

            selectionType={'multi'}
            selectedItems={selectedItems}
            ariaLabels={{
                selectionGroupLabel: "Items selection",
                allItemsSelectionLabel: ({selectedItems}) =>
                    `${selectedItems.length} ${
                        selectedItems.length === 1 ? "item" : "items"
                    } selected`,
                itemSelectionLabel: ({selectedItems}, item) => {
                    const isItemSelected = selectedItems.filter(
                        i => i.name === item.name
                    ).length;
                    return `${item.name} is ${
                        isItemSelected ? "" : "not"
                    } selected`;
                }
            }}
            sortingColumn={collectionProps.sortingColumn}

            trackBy={'replay_id'}
            items={items}
            filter={
                <Input
                    type="search"
                    value={filterProps.filteringText}
                    onChange={event => {
                        actions.setFiltering(event.detail.value);
                    }}
                    placeholder="Find replays..."
                />
            }
            stickyHeader
            loading={loading}
            loadingText="Loading resources"
            columnDefinitions={COL_DEF}
            pagination={<Pagination {...paginationProps}/>}


        ></Table>

    )

}