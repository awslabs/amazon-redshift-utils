import Input from "@awsui/components-react/input";
import Button from "@awsui/components-react/button";
import AppLayout from "@awsui/components-react/app-layout";
import React, {useEffect, useState} from "react";
import {Container, FormField, Header, SpaceBetween, TokenGroup} from "@awsui/components-react";
import ReplayList from "../components/ReplayList";
import AccessControl from "../components/AccessControl";

export const HomePage = () => {

    const [resource, setResource] = useState('');
    const [replays, setReplays] = useState([])
    const [buckets, setBuckets] = useState([])
    const [bucketLabels, setBucketLabels] = useState([])
    const [searching, setSearching] = useState(false)
    const [profiles, setProfiles] = useState([])
    const [valid, setValid] = useState(true)

    useEffect(() => {
        const fetchData = async () => {
            const response = await fetch(`/getprofile`);
            const newData = await response.json();
            setProfiles(newData.profiles);
        };
        fetchData();
    }, []);

    function search(uri) {
        // TODO: explicit s3 uri validation

        if (uri !== '' && uri.startsWith('s3://')) {
            setSearching(true);

            fetch(`/search?uri=${uri}`).then(response => response.json())
                .then(response => {
                    if (!response.success) {
                        setValid(false)
                    } else {
                        if (!buckets.includes(response.bucket)) {
                            setReplays(replays => [...replays, ...response.replays]);
                            setBuckets(buckets => [...buckets, response.bucket]);
                            setBucketLabels(buckets => [...buckets, {label: response.bucket}]);
                        }
                    }

                    setSearching(false);


                }).catch((error) => {
                console.error('Error:', error);
                setSearching(false);

            });
            setResource("");
        } else {
            setValid(false)

        }
    }

    /**
     * Removes entries from list of replays when bucket is removed
     * @param {number} itemIndex Total data set of query frequency values.
     */
    function removeBucket(itemIndex) {
        let bucket = bucketLabels[itemIndex].label
        setBucketLabels([...bucketLabels.slice(0, itemIndex),
            ...bucketLabels.slice(itemIndex + 1)]);
        setBuckets([...buckets.slice(0, itemIndex),
            ...buckets.slice(itemIndex + 1)]);
        let result = replays.filter((data) => {
            return data.bucket.search(bucket) === -1;
        });
        setReplays(result);
    }

    return (
        <AppLayout
            navigationHide={true}
            content={
                <Container
                    header={
                        <Header variant="h1" description="An analysis tool provided by Redshift.">
                            Simple Replay Analysis
                        </Header>
                    }>
                    <SpaceBetween size={"l"}>
                        <AccessControl profiles={profiles}></AccessControl>


                        <FormField label="Replay Bucket"
                                   errorText={!valid && "Unable to access S3. Please check the provided URI."}
                                   secondaryControl={
                                       <Button
                                           disabled={resource === ""}
                                           loading={searching}
                                           variant={'primary'}
                                           onClick={() => search(resource)}>
                                           Search
                                       </Button>}>

                            <Input value={resource}
                                   errorText="This is an error message."

                                   type={'search'}
                                   placeholder={"s3://bucket/prefix/object"}
                                   onChange={(event) => {
                                       setResource(event.detail.value);
                                       setValid(true)
                                   }}/>

                        </FormField>

                        <TokenGroup
                            onDismiss={({detail: {itemIndex}}) => {
                                removeBucket(itemIndex)
                            }}
                            items={bucketLabels}>

                        </TokenGroup>

                        <ReplayList search={searching} replays={replays}/>

                    </SpaceBetween>


                </Container>
            }

        />
    );

}

