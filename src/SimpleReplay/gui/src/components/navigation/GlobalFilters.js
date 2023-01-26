import React, {useState, useEffect} from 'react';
import {Box, Button, Header, Multiselect, SpaceBetween,} from "@awsui/components-react";
import Slider from '@mui/material/Slider';
import * as awsui from '@awsui/design-tokens';
import millisToMinutesAndSeconds from "../../helpers/msFormatter";
import prepareSelectOptions from "../../helpers/PrepareOptions";

/**
 * Global filters
 * Manipulates query type, user, and duration selection values
 * Updates global selectedQueryTypes, selectedUser, selectedDuration variables
 */
const GlobalFilters = ({selectedQueryTypes, setSelectedQueryTypes,
                           selectedUser, setSelectedUser,
                           selectedDuration, setSelectedDuration}) => {
    /** @prop selectedQueryTypes, array of selected "query type" options */
    /** @prop setSelectedQueryTypes, useState setter for selectedQueryTypes */
    /** @prop selectedUser, array of selected "user" options */
    /** @prop setSelectedUser, useState setter for selectedUser  */
    /** @prop selectedDuration, array of selected "duration" range in milliseconds. ex: [0,190290]  */
    /** @prop setSelectedDuration, useState setter for selectedDuration */

    /** Longest relative duration in milliseconds */
    const [maxDuration, setMaxDuration ] = useState(0);

    /** Array of user options from response data */
    const [selectUserOptions, setSelectUserOptions] = useState();

    useEffect(() => {
        const fetchData = async () => {
            const response = await fetch(`/time_range`);
            const newData = await response.json();

            setMaxDuration(newData.time);
            setSelectedDuration([0, maxDuration])
            setSelectUserOptions(prepareSelectOptions(newData.users))
        };
        fetchData();
    },
    [maxDuration, setSelectedDuration]);


    function clearFilter() {
        setSelectedQueryTypes(queryTypes)
        setSelectedUser([])
        setSelectedDuration([0, maxDuration])
    }

    return (
        <div style={boxStyle}>
            <Header variant={"h2"}>Filter Results: </Header>

            <SpaceBetween size={'m'}>
                <Multiselect selectedOptions={selectedQueryTypes}
                             options={queryTypes}
                             placeholder={"Filter by query types"}
                             onChange={({ detail }) => setSelectedQueryTypes(detail.selectedOptions)}
                />
                <Multiselect selectedOptions={selectedUser}
                             options={selectUserOptions}
                             placeholder={"Filter by user"}
                             onChange={({ detail }) => setSelectedUser(detail.selectedOptions)
                }/>
            </SpaceBetween>


            <Box>
                <h4 >Filter by time frame</h4>
                <Slider getAriaLabel={() => 'Range'}
                        valueLabelFormat={(value)=> `${millisToMinutesAndSeconds(value)}`}
                        value={selectedDuration}
                        min={0}
                        max={maxDuration}
                        onChange={ (event, newValue) => setSelectedDuration(newValue)}
                        size={'large'}
                        valueLabelDisplay="auto"
                        disableSwap
                        marks={[{ value: 0, label: millisToMinutesAndSeconds(0,0)},
                                {value: maxDuration, label: millisToMinutesAndSeconds(maxDuration,0)}]}

                />
            </Box>

            <Button onClick={() => clearFilter()}>Clear filters</Button>

        </div>

    )
};

/**
 * Custom styling for filters box, uses AWS-UI design tokens to mimic default styles
 * @const {object}
 */
const boxStyle = {
    position: 'sticky',
    top: 0,
    display: 'block',
    backgroundColor: awsui.colorBackgroundControlDefault,
    borderColor: awsui.colorBorderControlDefault,
    borderWidth: 2,
    padding: 20,
    boxShadow: 20,
    boxShadowColor:awsui.colorBorderControlDefault
};

/**
 * Array of query type options
 * @const {object}
 */
const queryTypes = [
    {
        label: "SELECT",
        value: "1",
    },
    {
        label: "INSERT",
        value: "2",
    },
    {
        label: "UPDATE",
        value: "3",
    },
    {
        label: "DELETE",
        value: "4",
    },
    {
        label: "COPY",
        value: "5",
    },
    {
        label: "UNLOAD",
        value: "6",
    },
    {
        label: "DDL",
        value: "7",
    },
    {
        label: "COMMAND",
        value: "8",
    },
    {
        label: "CTAS",
        value: "9",
    },
    {
        label: "UTILITY",
        value: "10",
    },
    {
        label: "OTHER",
        value: "11",
    }];

export default GlobalFilters;