import React, {useState} from 'react';
import Nav from "../components/navigation/NavDrawer"
import ReplayOverview from "../components/ReplayOverview";
import CompareThroughput from "../components/ReplayAnalysis/CompareThroughput"
import {AppLayout, ExpandableSection, Header, SpaceBetween} from "@awsui/components-react";
import TopRunningQueries from "../components/ReplayAnalysis/TopRunningQueries";
import QueryLatency from "../components/ReplayAnalysis/QueryLatency";
import AggregateMetrics from "../components/ReplayAnalysis/AggregateMetrics";
import ErrorTable from "../components/ReplayValidation/ErrorTable";
import ErrorDistribution from "../components/ReplayValidation/ErrorDistribution";
import GlobalFilters from "../components/navigation/GlobalFilters";
import TopQueryDeltas from "../components/ReplayAnalysis/TopQueryDeltas";
import CopyAgg from "../components/ReplayValidation/CopyAgg";
import ToolBar from "../components/navigation/ToolBar";

export const AnalysisPage = () => {

    const [navOpen, setNavOpen] = useState(false);
    const [toolsOpen, setToolsOpen] = useState(false);
    const [replays, setReplays] = useState([]);
    const [selectedQueryTypes, setSelectedQueryTypes] = useState([]);
    const [selectedUser, setSelectedUser] = useState([]);
    const [selectedDuration, setSelectedDuration] = useState([0, 0]);

    return (
        <AppLayout
            navigation={
                <Nav/>
            }
            navigationOpen={navOpen}
            tools={<ToolBar/>}
            onNavigationChange={() => setNavOpen(!navOpen)}
            onToolsChange={() => setToolsOpen(!toolsOpen)}


            content={
                <div>
                    <SpaceBetween size={'xxl'}>
                        <ReplayOverview replays={replays} setReplays={setReplays}/>
                        <GlobalFilters selectedQueryTypes={selectedQueryTypes}
                                       setSelectedQueryTypes={setSelectedQueryTypes}
                                       selectedUser={selectedUser} setSelectedUser={setSelectedUser}
                                       selectedDuration={selectedDuration} setSelectedDuration={setSelectedDuration}

                        />

                        <div id="analysis">
                            <ExpandableSection
                                variant="container"
                                header={
                                    <Header
                                        variant="h2"
                                        description="Query level analysis of the selected replays."
                                    >
                                        Analysis
                                    </Header>
                                }>


                                <SpaceBetween size={"xxl"}>

                                    <CompareThroughput selectedQueryTypes={selectedQueryTypes}
                                                       selectedDuration={selectedDuration}
                                                       selectedUser={selectedUser}
                                                       replays={replays}/>

                                    <AggregateMetrics selectedQueryTypes={selectedQueryTypes}
                                                      selectedUser={selectedUser}
                                                      selectedDuration={selectedDuration} replays={replays}/>

                                    <QueryLatency selectedQueryTypes={selectedQueryTypes}
                                                  selectedUser={selectedUser}
                                                  selectedDuration={selectedDuration} replays={replays}/>

                                    <TopRunningQueries selectedQueryTypes={selectedQueryTypes}
                                                       selectedUser={selectedUser}
                                                       selectedDuration={selectedDuration}
                                                       replays={replays}/>

                                    <TopQueryDeltas selectedQueryTypes={selectedQueryTypes}
                                                    selectedUser={selectedUser}
                                                    selectedDuration={selectedDuration}
                                                    replays={replays}/>

                                </SpaceBetween>
                            </ExpandableSection>
                        </div>
                        <div id="validation">
                            <ExpandableSection
                                variant="container"
                                header={
                                    <Header variant="h2"
                                            description="Error validation and consistency metrics across replays.">
                                        Validation
                                    </Header>
                                }>
                                <SpaceBetween size={"xxl"}>

                                    <ErrorTable selectedDuration={selectedDuration}
                                                selectedUser={selectedUser}
                                                replays={replays}/>

                                    <ErrorDistribution selectedDuration={selectedDuration}/>

                                    <CopyAgg selectedDuration={selectedDuration}
                                             selectedUser={selectedUser}
                                             replays={replays}/>
                                </SpaceBetween>
                            </ExpandableSection>
                        </div>

                    </SpaceBetween>
                </div>
            }

        />

    )
}
